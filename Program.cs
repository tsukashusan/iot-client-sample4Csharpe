using System.Text;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

const double MinTemperature = 20;
const double MinHumidity = 60;
Random Rand = new Random();
ulong _messageId = 0;
IConfiguration configuration = new ConfigurationBuilder()
      .SetBasePath(Directory.GetCurrentDirectory())
      .AddJsonFile("appsettings.json", true, true)
      .Build();


var appInsightSection = configuration.GetSection("ApplicationInsight");

var telemetryconfiguration = TelemetryConfiguration.CreateDefault();
telemetryconfiguration.ConnectionString = appInsightSection["ConnectionString"];
var telemetryClient = new TelemetryClient(telemetryconfiguration);
Console.WriteLine("IoT Client Start!");
telemetryClient.TrackTrace("Hello World!");

var iotSection = configuration.GetSection("iothub");
var mode = iotSection["mode"];
IotHubDeviceClient _deviceClient = new IotHubDeviceClient(iotSection["ConnectionString"]);
var devicename = iotSection["deviceName"];
var fileNamePrefix = iotSection["fileNamePrefix"];
var batchSize = iotSection["batchSize"];

var modeswitch = mode switch
{
    "realtime" => SendDeviceToCloudMessagesAsync(),
    "batch" => SendDeviceToCloudMessagesAsync4Batch(fileNamePrefix, uint.Parse(batchSize)),
    _ => throw new Exception("unknow mode")
};

await modeswitch;

async Task SendDeviceToCloudMessagesAsync()
{
    await _deviceClient.OpenAsync();

    while (true)
    {
        try
        {
            var currentTemperature = MinTemperature + Rand.NextDouble() * 15;
            var currentHumidity = MinHumidity + Rand.NextDouble() * 20;
            if (_messageId == ulong.MaxValue) _messageId = 0;
            var telemetryDataPoint = new
            {
                messageId = _messageId++,
                deviceId = devicename,
                temperature = Math.Round(currentTemperature, 2),
                humidity = Math.Round(currentHumidity, 2)
            };
            var messageString = JsonConvert.SerializeObject(telemetryDataPoint);
            var message = new TelemetryMessage(messageString);
            message.Properties.Add("temperatureAlert", (currentTemperature > 30) ? "true" : "false");
            await _deviceClient.SendTelemetryAsync(message);
            Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);
            telemetryClient.TrackTrace($"{DateTime.Now} > Sending message: {messageString}");
            await Task.Delay(500);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            Console.WriteLine(e.StackTrace);
            Console.WriteLine("Suspend:{0} msec", 1000);
            telemetryClient.TrackException(e);
            await Task.Delay(1000);
            _messageId = 0;
            continue;
        }
    }
}

async Task SendDeviceToCloudMessagesAsync4Batch(string fileNamePrefix, uint batchSize)
{
    await _deviceClient.OpenAsync();
    Func<string, string> getFileName = (prefix) => $"{prefix}_{DateTime.Now.ToString("yyyyMMddHHmmssfff")}.json";
    string fileName = getFileName(fileNamePrefix);

    for (MemoryStream memStream = new MemoryStream(10240); ;)
    {
        try
        {
            var currentTemperature = MinTemperature + Rand.NextDouble() * 15;
            var currentHumidity = MinHumidity + Rand.NextDouble() * 20;
            if (_messageId == ulong.MaxValue) _messageId = 0;
            var telemetryDataPoint = new
            {
                messageId = _messageId++,
                deviceId = devicename,
                temperature = Math.Round(currentTemperature, 2),
                humidity = Math.Round(currentHumidity, 2)
            };
            var messageString = JsonConvert.SerializeObject(telemetryDataPoint);
            byte[] messagebytearray = Encoding.UTF8.GetBytes(messageString);
            var message = new TelemetryMessage(messagebytearray);
            message.Properties.Add("temperatureAlert", (currentTemperature > 30) ? "true" : "false");
            Console.WriteLine($"{DateTime.Now} > Write message 4 batch {fileName}: {messageString}");
            telemetryClient.TrackTrace($"{DateTime.Now} > Write message 4 batch {fileName}: {messageString}");
            await memStream.WriteAsync(Encoding.UTF8.GetBytes(messageString + Environment.NewLine));

            if ((_messageId != 0 && _messageId % batchSize == 0) || _messageId == ulong.MaxValue)
            {
                var fileUploadSasUriRequest = new FileUploadSasUriRequest(fileName);

                // Lines removed for clarity
                FileUploadSasUriResponse sasUri = await _deviceClient.GetFileUploadSasUriAsync(fileUploadSasUriRequest);
                Uri uploadUri = sasUri.GetBlobUri();

                var blockBlobClient = new BlockBlobClient(uploadUri);
                memStream.Position = 0;
                await blockBlobClient.UploadAsync(memStream);
                var successfulFileUploadCompletionNotification = new FileUploadCompletionNotification(sasUri.CorrelationId, true)
                {
                    // Optional, user defined status code. Will be present when service client receives this file upload notification
                    StatusCode = 200,

                    // Optional, user-defined status description. Will be present when service client receives this file upload notification
                    StatusDescription = "Success"
                };
                await _deviceClient.CompleteFileUploadAsync(successfulFileUploadCompletionNotification);
                var msg = $"upload {fileName} successful!";
                telemetryClient.TrackTrace(msg);
                Console.WriteLine(msg);
                await memStream.DisposeAsync();
                memStream = new MemoryStream(10240);
                fileName = getFileName(fileNamePrefix);
            }
            await Task.Delay(500);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            Console.WriteLine(e.StackTrace);
            Console.WriteLine("Suspend:{0} msec", 1000);
            telemetryClient.TrackException(e);
            await Task.Delay(1000);
            _messageId = 0;
            continue;
        }
    }
}