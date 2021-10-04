using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace sck2eventhub
{
    class Program
    {
        private enum ScDeviceId
        {
            VDK09   = 12613,    // https://smartcitizen.me/kits/12613
            VDK05   = 12611,    // https://smartcitizen.me/kits/12611
        }

        private enum ScSensorId
        {
            TVOC        = 113,
            ECO2        = 112,
            LIGHT       = 14,
            BATTERY     = 10,
            NOISE       = 53,
            PRESSURE    = 58,
            PM1_0       = 89,
            PM2_5       = 87,
            PM10_0      = 88,
            HUMIDITY    = 56,
            TEMPERATURE = 55,
        }

        private static readonly ScDeviceId[] SC_DEVICES =
        {
            ScDeviceId.VDK09, ScDeviceId.VDK05
        };
        private static readonly ScSensorId[] SC_SENSORS =
        {
            ScSensorId.ECO2,
            ScSensorId.LIGHT,
            ScSensorId.NOISE,
            ScSensorId.PRESSURE,
            ScSensorId.PM2_5,
            ScSensorId.HUMIDITY,
            ScSensorId.TEMPERATURE,
        };

        private static readonly DateTime FROM_DATE = new DateTime(2021, 7, 1, 0, 0, 0, DateTimeKind.Utc);
        private static readonly DateTime TO_DATE = new DateTime(2021, 10, 1, 0, 0, 0, DateTimeKind.Utc);
        private const string SC_ROLLUP = "1s";

        private const string AZURE_EVENT_HUB_CONNECTION_STRING = "";
        private const string AZURE_EVENT_HUB_NAME = "";

        private class SensorData
        {
            public ScDeviceId DeviceId { set; get; }
            public ScSensorId SensorId { set; get; }
            public DateTime Timestamp { set; get; }
            public double SensorValue { set; get; }

            public string ToJsonString()
            {
                return $"{{\"deviceId\":\"{DeviceId.ToString()}\",\"timestamp\":\"{Timestamp:O}\",\"{SensorId.ToString().ToLower()}\":{SensorValue}}}";
            }

        }

        private static async Task<List<SensorData>> ReadSensorData(ScDeviceId deviceId, ScSensorId sensorId, DateTime fromDate, DateTime toDate)
        {
            using var client = new HttpClient();
            using var response = await client.GetAsync($"https://api.smartcitizen.me/v0/devices/{(int)deviceId}/readings?sensor_id={(int)sensorId}&rollup={SC_ROLLUP}&from={fromDate:O}&to={toDate:O}");
            var content = await response.Content.ReadAsStringAsync();

            using var jsonDoc = JsonDocument.Parse(content);
            var readings = jsonDoc.RootElement.GetProperty("readings");

            var sensorData = new List<SensorData>();
            foreach (var readData in readings.EnumerateArray())
            {
                sensorData.Add(new SensorData { DeviceId = deviceId, SensorId = sensorId, Timestamp = readData[0].GetDateTime(), SensorValue = readData[1].GetDouble(), });
            }

            return sensorData;
        }

        private static async Task WriteSensorData(List<SensorData> sensorData)
        {
            if (sensorData.Count <= 0) return;

            var client = new EventHubProducerClient(AZURE_EVENT_HUB_CONNECTION_STRING, AZURE_EVENT_HUB_NAME);
            try
            {
                EventDataBatch eventBatch = null;
                foreach (var data in sensorData)
                {
                    if (eventBatch == null) eventBatch = await client.CreateBatchAsync();
                    if (!eventBatch.TryAdd(new EventData(data.ToJsonString())))
                    {
                        await client.SendAsync(eventBatch);
                        eventBatch.Dispose();

                        eventBatch = await client.CreateBatchAsync();
                        eventBatch.TryAdd(new EventData(data.ToJsonString()));
                    }
                }
                await client.SendAsync(eventBatch);
                eventBatch.Dispose();
            }
            finally
            {
                await client.DisposeAsync();
            }
        }

        static async Task MainAsync(string[] args)
        {
            // Smart Citizen Platfromからセンサーデータを読み出す
            var sensorData = new List<SensorData>();
            foreach (var deviceId in SC_DEVICES)
            {
                foreach (var sensorId in SC_SENSORS)
                {
                    sensorData.AddRange(await ReadSensorData(deviceId, sensorId, FROM_DATE, TO_DATE));
                }
            }
            Console.WriteLine($"sensorData.Count = {sensorData.Count}");

            // センサーデータを日時で昇順
            sensorData.Sort((a, b) => { var diff = a.Timestamp.Ticks - b.Timestamp.Ticks; return diff > 0 ? 1 : diff < 0 ? -1 : 0; });

            // センサーデータをAzure Event Hubへ送信
            await WriteSensorData(sensorData);
        }

        static void Main(string[] args)
        {
            var sw = new Stopwatch();
            sw.Restart();
            MainAsync(args).Wait();
            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalMinutes);
        }
    }
}
