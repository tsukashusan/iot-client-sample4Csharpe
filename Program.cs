using System.Text;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;

// See https://aka.ms/new-console-template for more information

const string DeviceId = "myFirstDevice4Csharp";
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
IotHubDeviceClient _deviceClient = new IotHubDeviceClient(iotSection["ConnectionString"]);

await SendDeviceToCloudMessagesAsync();

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
                deviceId = DeviceId,
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