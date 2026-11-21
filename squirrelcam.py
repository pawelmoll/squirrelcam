import asyncio
import csi
import ds18x20
import logging
import machine
import microdot
import micropython
import mqtt
import network
import ntptime
import onewire
import os
import sys
import time
import uhome

import config

micropython.alloc_emergency_exception_buf(256)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log_delete_request = False
log_flush_evt = asyncio.Event()

camera = None
ha_device = None
http = microdot.Microdot()


@http.route("/")
async def index(request):
    log.info(request.url)
    return microdot.send_file(f"{config.sdcard_path}/index.html", content_type="text/html")

@http.get("/snapshot.jpg")
async def get_snapshot(request):
    log.info(request.url)
    camera.trigger()
    await camera.ready_evt.wait()
    return microdot.Response(
        body=camera.img.bytearray(),
        headers={'Content-Type': 'image/jpeg'},
    )

@http.get("/log.txt")
async def get_log(request):
    log.info(request.url)
    log_flush_evt.set()
    while log_flush_evt.is_set():
        await asyncio.sleep_ms(10);
    return microdot.send_file(f"{config.sdcard_path}/{config.log_file_name}", content_type="text/plain", max_age=0)

@http.delete("/log.txt")
async def delete_log(request):
    global log_delete_request
    log.info(request.url)
    log_delete_request = True
    log_flush_evt.set()
    return "Ok", 202


class Camera():
    def __init__(self, device, path, burst_length, burst_interval_ms):
        self.path = path
        self.burst_length = burst_length
        self.burst_interval_ms = burst_interval_ms

        self.csi = csi.CSI(stream=False)
        self.csi.reset()
        self.csi.pixformat(csi.RGB565) # possible: RGB565 GRAYSCALE (grayscale not supported by N6 VENC)
        self.csi.framesize(csi.WXGA) # possible: QVGA VGA WXGA

        self.trigger_evt = asyncio.Event()
        self.ready_evt = asyncio.Event()

        self.streaming = False

        self.img = None
        self.img_serial = 0

        self.publish = False
        self.image = uhome.Image(device, "Snapshot", url_topic=False, device_class="image", content_type="image/jpeg")

    async def loop(self):
        log.info("Starting camera handling")
        while True:
            self.ready_evt.clear()
            if not self.streaming:
                await self.trigger_evt.wait()
            self.trigger_evt.clear()
            log.info("Camera start")
            t1 = time.ticks_ms()
            img = None
            while img is None:
                img = self.csi.snapshot(blocking=False)
                await asyncio.sleep_ms(int(1000/24/10))
            td = time.ticks_diff(time.ticks_ms(), t1)
            log.info(f"Got image in {td} ms")
            if self.streaming:
                self.csi.snapshot(blocking=False)
            t1 = time.ticks_ms()
            self.img = img.compress(quality=85)
            self.img_serial += 1
            td = time.ticks_diff(time.ticks_ms(), t1)
            log.info(f"Compressed in {td} ms")
            if self.publish:
                self.image.publish_image(img.bytearray())
                log.info("Published")
                self.publish = False
            self.ready_evt.set()

    def run(self):
        self.task = asyncio.create_task(self.loop())

    def trigger(self):
        log.info("Trigger!")
        if not self.streaming:
            self.ready_evt.clear()
            self.trigger_evt.set()

    async def burst(self):
        length = self.burst_length
        while length > 0:
            length -= 1
            self.publish = True
            self.trigger()
            await asyncio.sleep_ms(self.burst_interval_ms)


class Temperature():
    def __init__(self, pin, device, interval_ms):
        self.convertion_delay_ms = 750
        self.interval_ms = max(interval_ms - self.convertion_delay_ms, 250)
        self.ds = ds18x20.DS18X20(onewire.OneWire(pin))
        self.roms = self.ds.scan()
        self.sensors = dict()
        for rom in self.roms:
            code = rom.hex()
            log.info(f"Detected DS18X20 {code}")
            if len(self.roms) > 1:
                name = f"Temperature {code}"
            else:
                name = "Temperature"
            self.sensors[code] = uhome.Sensor(device, name, device_class="temperature", unit_of_measurement="°C")

    async def loop(self):
        log.info("Starting temperature measurements")
        while True:
            self.ds.convert_temp()
            await asyncio.sleep_ms(self.convertion_delay_ms)
            for rom in self.roms:
                code = rom.hex()
                t = self.ds.read_temp(rom)
                log.debug(f"Temperature {code}: {t}")
                self.sensors[code].publish(f"{t:.1f}")
            await asyncio.sleep_ms(self.interval_ms)

    def run(self):
        self.task = asyncio.create_task(self.loop())


class Motion():
    def __init__(self, pin, device):
        self.pin = pin
        self.pin.init(pin.IN)
        self.pin.irq(handler=self.irq_handler, trigger=machine.Pin.IRQ_RISING, hard=True)
        self.binary_sensor = uhome.BinarySensor(device, "Motion", device_class="motion")
        self.flag = asyncio.ThreadSafeFlag()
        self.flag.set()

    def irq_handler(self, pin):
        self.flag.set()

    async def loop(self):
        log.info("Starting motion detection")
        while True:
            await self.flag.wait()
            state = self.pin.value()
            log.info("Motion!" if state else "No motion...")
            self.binary_sensor.publish("ON" if state else "OFF")
            if state:
                await camera.burst()
                self.flag.set()

    def run(self):
        self.task = asyncio.create_task(self.loop())


class Signal():
    def __init__(self, device, wlan, interval_s=60):
        self.interval_s = interval_s
        self.sensor = uhome.Sensor(device, "Signal Strength", device_class="signal_strength", unit_of_measurement="dBm", entity_category="diagnostic", enabled_by_default="false")
        self.wlan = wlan

    async def loop(self):
        log.info("Starting signal reporting")
        while True:
            try:
                signal = "%.0f" % self.wlan.status("rssi")
            except:
                signal = "None"
            log.debug(f"Signal {signal}")
            self.sensor.publish(signal)
            await asyncio.sleep(self.interval_s)

    def run(self):
        self.task = asyncio.create_task(self.loop())


class TimeSync():
    def __init__(self, interval_s=60*60):
        self.interval_s = interval_s

    async def loop(self):
        log.info("Starting time synchronisation")
        while True:
            try:
                ntptime.settime()
                log.debug(f"Time now {time.localtime()}")
            except Exception as e:
                log.error(f"Failed to set time from NTP! ({e})")
            await asyncio.sleep(self.interval_s)

    def run(self):
        self.task = asyncio.create_task(self.loop())



class SDCardHandler(logging.StreamHandler):
    def __init__(self, filename, mode="a", encoding="UTF-8", flush_interval_s=5*60):
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.flush_interval_s = flush_interval_s
        super().__init__(stream=open(self.filename, mode=self.mode, encoding=self.encoding))

    async def loop(self):
        global log_delete_request
        while True:
            try:
                await asyncio.wait_for(log_flush_evt.wait(), self.flush_interval_s)
            except asyncio.TimeoutError:
                pass
            finally:
                log.debug("Flush!")
                try:
                    self.stream.close()
                    if log_delete_request:
                        mode = "w";
                        log_delete_request = False
                    else:
                        mode = "a";
                    self.stream = open(self.filename, mode=mode, encoding=self.encoding)
                except Exception as e:
                    log.error(f"Failed to flush! ({e})")
                log_flush_evt.clear()

    def run(self):
        self.task = asyncio.create_task(self.loop())


class MQTTHandler(logging.Handler):
    def __init__(self, device):
        super().__init__()
        self.sensor = uhome.Sensor(device, "Diagnostic Messages", entity_category="diagnostic", enabled_by_default="false")

    def emit(self, record):
        if record.levelno >= self.level:
            self.sensor.publish(self.format(record))


async def main():
    global camera

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(stdout_handler)

    logfile_handler = SDCardHandler(f"{config.sdcard_path}/{config.log_file_name}")
    logfile_handler.setLevel(logging.DEBUG)
    logfile_handler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(message)s"))
    logfile_handler.run()
    log.addHandler(logfile_handler)

    ha_device = uhome.Device(config.ha_device_name, mf=config.ha_manufacturer, mdl=config.ha_model)

    leds = machine.Pin("P7", machine.Pin.OUT);
    leds.on()

    network.hostname(config.host_name)
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(config.wifi_ssid, config.wifi_key)
    while not wlan.isconnected():
        log.info(f"Trying to connect to '{config.wifi_ssid}'... ({wlan.status()})")
        time.sleep_ms(1000)
    log.info(f"WiFi Connected {wlan.ifconfig()}")

    time_sync = TimeSync()
    time_sync.run()

    mqttc = mqtt.MQTTClient(ha_device.id, config.mqtt_host, config.mqtt_port, user=config.mqtt_username, password=config.mqtt_password, keepalive=60)
    while not ha_device.connect(mqttc):
        log.info(f"Trying to connect to MQTT broker {config.mqtt_host}:{config.mqtt_port} as {config.mqtt_username}")
        time.sleep_ms(1000)
    log.info(f"Connected to MQTT broker {mqttc}")

    mqtt_handler = MQTTHandler(ha_device)
    mqtt_handler.setLevel(logging.INFO)
    mqtt_handler.setFormatter(logging.Formatter("%(message)s"))

    reset_cause = uhome.Sensor(ha_device, "Last Reset Cause", device_class="enum", entity_category="diagnostic", enabled_by_default="false")
    signal = Signal(ha_device, wlan)
    temperature = Temperature(machine.Pin("P6"), ha_device, config.temperature_interval_ms)
    camera = Camera(ha_device, config.sdcard_path, config.camera_burst_length, config.camera_burst_inverval_ms)
    motion = Motion(machine.Pin("P9"), ha_device)

    ha_device.discover_all()

    log.addHandler(mqtt_handler)
    reset_causes = {
        machine.PWRON_RESET: "Power On",
        machine.WDT_RESET: "Watchdog",
        machine.SOFT_RESET: "Soft Reset",
        machine.HARD_RESET: "Hard Reset",
        machine.DEEPSLEEP_RESET: "Deep Sleep Reset",
    }
    reset_cause.publish(reset_causes.get(machine.reset_cause(), "None"))
    signal.run()
    temperature.run()
    camera.run()
    motion.run()

    server = asyncio.create_task(http.start_server(host=config.http_host, port=config.http_port))
    while True:
        if wlan.isconnected():
            ha_device.loop()
        else:
            log.warning("Disconnected from WiFi")
        await asyncio.sleep_ms(100)
    log.error("Should never get here!")
    await server

asyncio.run(main())
log.error("Should never get here either!")
