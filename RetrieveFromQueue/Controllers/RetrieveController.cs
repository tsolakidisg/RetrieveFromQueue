using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Rmq.Publisher.Models;
using System;
using System.Text;

namespace RetrieveFromQueue.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RetrieveController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public RetrieveController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet]
        public IActionResult Get()
        {
            // Create a connection factory, using the RabbitMQ configuration settings from appsettings.json
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = _configuration["RabbitMqConnection:HostName"].ToString(),
                VirtualHost = _configuration["RabbitMqConnection:VirtualHost"].ToString(),
                Port = Convert.ToInt32(_configuration["RabbitMqConnection:Port"]),
                UserName = _configuration["RabbitMqConnection:Username"].ToString(),
                Password = _configuration["RabbitMqConnection:Password"].ToString()
            };

            IConnection conn = factory.CreateConnection();
            IModel channel = conn.CreateModel();

            // Queue name to retrieve from
            string consumeQueue = "consumerQueue";

            // Initialize the message string and the number of retries
            string message = string.Empty;
            int retries;
            // Max 3 retries to read from the queue
            for (retries = 1; retries <= 3; retries++)
            {
                try
                {
                    // Get the message from the queue without acknowledging it yet
                    BasicGetResult result = channel.BasicGet(consumeQueue, false);
                    if (result != null)
                    {
                        // Decode the received message, deserialize it into a JSON object and acknowledge its processing
                        message = Encoding.UTF8.GetString(result.Body.Span);
                        ResponseModel response = JsonConvert.DeserializeObject<ResponseModel>(message);
                        channel.BasicAck(result.DeliveryTag, false);
                        retries = 10; // Set retries to 10 (>3) to exit the loop and use it later as a flag to depict the message receival
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
                
            }

            if (retries >= 10)
            {
                // The message was retrieved successfully
                return Ok(message);
            }
            else
            {
                // The message was not found
                return NotFound("Order not found (Max retries: 3 reached).");
            }
        }
    }
}
