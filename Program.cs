using System.Text;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;

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
await SendDeviceToCloudMessagesAsync();
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
            var message = new TelemetryMessage(Encoding.ASCII.GetBytes(messageString));
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
    string fileName = $"{fileNamePrefix}_{DateTime.Now.ToString("yyyyMMddHHmmssSSS")}.txt";
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
            var message = new TelemetryMessage(Encoding.ASCII.GetBytes(messageString));
            message.Properties.Add("temperatureAlert", (currentTemperature > 30) ? "true" : "false");

            Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);
            telemetryClient.TrackTrace($"{DateTime.Now} > Sending message: {messageString}");
            await write2File(messageString, fileName);
            if ((_messageId != 0 && _messageId % batchSize == 0) || _messageId == ulong.MaxValue)
            {
                var fileUploadSasUriRequest = new FileUploadSasUriRequest("");

                // Lines removed for clarity
                FileUploadSasUriResponse sasUri = await _deviceClient.GetFileUploadSasUriAsync(fileUploadSasUriRequest);
                Uri uploadUri = sasUri.GetBlobUri();

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
                deleteFile(fileName);
                fileName = $"{fileNamePrefix}_{DateTime.Now.ToString("yyyyMMddHHmmssSSS")}.txt";
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
    async Task write2File(string message, string fileName)
    {
        // Set a variable to the Documents path.
        string docPath = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        // Write the specified text asynchronously to a new file named "WriteTextAsync.txt".
        using (StreamWriter outputFile = new StreamWriter(Path.Combine(docPath, fileName), true))
        {
            await outputFile.WriteAsync(message);
        }
    }
    void deleteFile(string fileName)
    {
        // Set a variable to the Documents path.
        string docPath = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        File.Delete(Path.Combine(docPath, fileName));
    }
}