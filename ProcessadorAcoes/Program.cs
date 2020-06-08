using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using Serilog;
using Serilog.Core;
using ProcessadorAcoes.Models;
using ProcessadorAcoes.Validators;

namespace ProcessadorAcoes
{
    class Program
    {
        private static ISubscriptionClient _subscriptionClient;
        private static Logger _logger;
        private static ConnectionMultiplexer _conexaoRedis;

        async static Task Main(string[] args)
        {
            _logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            _logger.Information("Testando o consumo de mensagens com Azure Service Bus");

            _logger.Information("Carregando configurações...");
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"appsettings.json");
            var configuration = builder.Build();

            string nomeTopic = configuration["AzureServiceBus:Topic"];
            string subscription = configuration["AzureServiceBus:Subscription"];
            _subscriptionClient = new SubscriptionClient(
                configuration["AzureServiceBus:ConnectionString"],
                nomeTopic, subscription);

            _logger.Information($"Topic = {nomeTopic}");
            _logger.Information($"Subscription = {nomeTopic}");

            _conexaoRedis =
                ConnectionMultiplexer.Connect(configuration["BaseCotacoes"]);
            
            _logger.Information("Aguardando mensagens...");
            _logger.Information("Pressione Enter para encerrar");
            RegisterOnMessageHandlerAndReceiveMessages();
            
            Console.ReadLine();
            await _subscriptionClient.CloseAsync();
            _logger.Warning("Encerrando o processamento de mensagens!");
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(
                ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _subscriptionClient.RegisterMessageHandler(
                ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            string dados = Encoding.UTF8.GetString(message.Body);
            _logger.Information($"Mensagem recebida: {dados}");

            var acao = JsonSerializer.Deserialize<Acao>(dados,
                new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });

            var validationResult = new AcaoValidator().Validate(acao);
            if (validationResult.IsValid)
            {
                acao.UltimaAtualizacao = DateTime.Now;
                var dbRedis = _conexaoRedis.GetDatabase();
                dbRedis.StringSet(
                    "ACAO-" + acao.Codigo,
                    JsonSerializer.Serialize(acao),
                    expiry: null);

                _logger.Information("Ação registrada com sucesso!");
            }
            else
            {
                _logger.Error("Dados inválidos para a Ação");
            }

            await _subscriptionClient.CompleteAsync(
                message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            _logger.Error($"Message handler - Tratamento - Exception: {exceptionReceivedEventArgs.Exception}.");

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            _logger.Error("Exception context - informaçoes para resolução de problemas:");
            _logger.Error($"- Endpoint: {context.Endpoint}");
            _logger.Error($"- Entity Path: {context.EntityPath}");
            _logger.Error($"- Executing Action: {context.Action}");

            return Task.CompletedTask;
        }
    }
}