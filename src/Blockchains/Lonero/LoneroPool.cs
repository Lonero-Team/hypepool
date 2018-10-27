#region license
// 
//      hypepool
//      https://github.com/bonesoul/hypepool
// 
//      Copyright (c) 2013 - 2018 Hüseyin Uslu
// 
//      Permission is hereby granted, free of charge, to any person obtaining a copy
//      of this software and associated documentation files (the "Software"), to deal
//      in the Software without restriction, including without limitation the rights
//      to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//      copies of the Software, and to permit persons to whom the Software is
//      furnished to do so, subject to the following conditions:
// 
//      The above copyright notice and this permission notice shall be included in all
//      copies or substantial portions of the Software.
// 
//      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//      OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//      SOFTWARE.
#endregion

using System;
using System.Net;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Hypepool.Common.Coins;
using Hypepool.Common.Daemon;
using Hypepool.Common.Factories.Server;
using Hypepool.Common.JsonRpc;
using Hypepool.Common.Mining.Context;
using Hypepool.Common.Native;
using Hypepool.Common.Pools;
using Hypepool.Common.Stratum;
using Hypepool.Common.Utils.Helpers.Time;
using Hypepool.Lonero.Daemon.Requests;
using Hypepool.Lonero.Daemon.Responses;
using Hypepool.Lonero.Stratum;
using Hypepool.Lonero.Stratum.Requests;
using Hypepool.Lonero.Stratum.Responses;
using Newtonsoft.Json;
using Serilog;

namespace Hypepool.Lonero
{
    public class LoneroPool : PoolBase<LoneroShare>
    {
        private ulong _poolAddressBase58Prefix;

        public LoneroPool(IServerFactory serverFactory)
            : base(serverFactory)
        {
            _logger = Log.ForContext<LoneroPool>().ForContext("Pool", "LNR");
        }

        public override async Task Initialize()
        {
            _logger.Information($"initializing pool..");

            try
            {
                PoolContext = new LoneroPoolContext();

                var miningDaemon = new DaemonClient("127.0.0.1", 34414, "user", "pass", LoneroConstants.DaemonRpcLocation);
                var wallDaemon = new DaemonClient("127.0.0.1", 34415, "user", "pass", LoneroConstants.DaemonRpcLocation);
                var jobManager = new LoneroJobManager();
                var stratumServer = ServerFactory.GetStratumServer();

                ((LoneroPoolContext)PoolContext).Configure(miningDaemon, wallDaemon, jobManager, stratumServer); // configure the pool context.
                PoolContext.JobManager.Configure(PoolContext);

                await RunPreInitChecksAsync(); // any pre-init checks.

                PoolContext.Daemon.Initialize(); // initialize mining daemon.
                ((LoneroPoolContext)PoolContext).WalletDaemon.Initialize(); // initialize wallet daemon.
                await WaitDaemonConnection(); // wait for coin daemon connection.
                await EnsureDaemonSynchedAsync(); // ensure the coin daemon is synced to network.

                await RunPostInitChecksAsync(); // run any post init checks required by the blockchain.
            }
            catch (Exception ex)
            {
                _logger.Fatal(ex.Message);
            }
        }

        public override async Task Start()
        {
            try
            {
                await PoolContext.JobManager.Start();

                ((LoneroJobManager)PoolContext.JobManager).JobQueue.Subscribe(x => BroadcastJob((LoneroJob)x));
                await ((LoneroJobManager)PoolContext.JobManager).JobQueue.Take(1).ToTask(); // wait for the first blocktemplate.

                PoolContext.StratumServer.Start(this);
            }
            catch (Exception ex)
            {
                _logger.Fatal(ex.Message);
            }
        }

        private void BroadcastJob(LoneroJob job)
        {
            _logger.Information($"Broadcasting new job 0x{job.Id:x8}..");

            PoolContext.StratumServer.ForEachClient(client =>
            {
                var context = client.GetContextAs<LoneroWorkerContext>(); // get client context.
                if (!context.IsAuthorized || !context.IsSubscribed) // if client is not authorized or subscribed yet,
                    return; // skip him.

                // check for miner activity timeout.
                var timeout = 600; // secs. todo: move this to config.
                var lastActivity = MasterClock.Now - context.LastActivity;
                if (timeout > 0 && lastActivity.TotalSeconds > timeout) // if the client had no activity for the period. 
                {
                    PoolContext.StratumServer.DisconnectClient(client); // kick him out.
                    return; // skip him for the new job.
                }

                var workerJob = ((LoneroJobManager) PoolContext.JobManager).CurrentJob.CreateWorkerJob(client);
                // todo: send it.
            });
        }

        protected override async Task RunPreInitChecksAsync()
        {
            // decode configured pool address.
            _poolAddressBase58Prefix = LibCryptonote.DecodeAddress(PoolContext.PoolAddress);
            if (_poolAddressBase58Prefix == 0)
                throw new PoolStartupAbortedException("unable to decode configured pool address!");
        }

        protected override async Task RunPostInitChecksAsync()
        {
            var infoResponse = await PoolContext.Daemon.ExecuteCommandAsync(LoneroRpcCommands.GetInfo);
            var addressResponse = await ((LoneroPoolContext)PoolContext).WalletDaemon.ExecuteCommandAsync<GetAddressResponse>(LoneroWalletCommands.GetAddress);

            // ensure pool owns wallet
            if (addressResponse.Response?.Address != PoolContext.PoolAddress)
                throw new PoolStartupAbortedException("pool wallet does not own the configured pool address!");

            // todo: add pool address verification.
        }

        public async Task WaitDaemonConnection()
        {
            while (!await IsDaemonConnectionHealthyAsync())
            {
                _logger.Information("waiting for wallet daemon connectivity..");
                await Task.Delay(TimeSpan.FromSeconds(5000));
            }

            _logger.Information("established coin daemon connection..");

            while (!await IsDaemonConnectedToNetworkAsync())
            {
                _logger.Information("waiting for coin daemon to connect peers..");
                await Task.Delay(TimeSpan.FromSeconds(5000));
            }

            _logger.Information("coin daemon do have peer connections..");
        }

        protected override async Task<bool> IsDaemonConnectionHealthyAsync()
        {
            var response = await PoolContext.Daemon.ExecuteCommandAsync<GetInfoResponse>(LoneroRpcCommands.GetInfo); // getinfo.

            // check if we are free of any errors.
            if (response.Error == null) // if so we,
                return true; // we have a healthy connection.

            if (response.Error.InnerException?.GetType() != typeof(DaemonException)) // if it's a generic exception
                _logger.Warning($"daemon connection problem: {response.Error.InnerException?.Message}");
            else // else if we have a daemon exception.
            {
                var exception = (DaemonException)response.Error.InnerException;

                _logger.Warning(exception.Code == HttpStatusCode.Unauthorized // check for credentials errors.
                    ? "daemon connection problem: invalid rpc credentials."
                    : $"daemon connection problem: {exception.Code}");
            }

            return false;
        }

        protected override async Task<bool> IsDaemonConnectedToNetworkAsync()
        {
            var response = await PoolContext.Daemon.ExecuteCommandAsync<GetInfoResponse>(LoneroRpcCommands.GetInfo); // getinfo.

            return response.Error == null && response.Response != null && // check if coin daemon have any incoming + outgoing connections.
                   (response.Response.OutgoingConnectionsCount + response.Response.IncomingConnectionsCount) > 0;
        }

        protected override async Task EnsureDaemonSynchedAsync()
        {
            var request = new GetBlockTemplateRequest
            {
                WalletAddress = PoolContext.PoolAddress,
                ReserveSize = LoneroConstants.ReserveSize
            };

            while (true) // loop until sync is complete.
            {
                var blockTemplateResponse = await PoolContext.Daemon.ExecuteCommandAsync<GetBlockTemplateResponse>(LoneroRpcCommands.GetBlockTemplate, request);

                var isSynched = blockTemplateResponse.Error == null || blockTemplateResponse.Error.Code != -9; // is daemon synced to network?

                if (isSynched) // break out of the loop once synched.
                    break;

                var infoResponse = await PoolContext.Daemon.ExecuteCommandAsync<GetInfoResponse>(LoneroRpcCommands.GetInfo); // getinfo.
                var currentHeight = infoResponse.Response.Height;
                var totalBlocks = infoResponse.Response.TargetHeight;
                var percent = (double) currentHeight / totalBlocks * 100;

                _logger.Information($"waiting for blockchain sync [{percent:0.00}%]..");

                await Task.Delay(5000); // stay awhile and listen!
            }        
            
            _logger.Information("blockchain is synched to network..");
        }

        protected override WorkerContext CreateClientContext()
        {
            return new LoneroWorkerContext();
        }

        public override async Task OnRequestAsync(IStratumClient client, Timestamped<JsonRpcRequest> timeStampedRequest)
        {
            var request = timeStampedRequest.Value;
            var context = client.GetContextAs<LoneroWorkerContext>();

            switch (request.Method)
            {
                case LoneroStratumMethods.Login:
                    OnLogin(client, timeStampedRequest);
                    break;
                case LoneroStratumMethods.GetJob:
                    OnGetJob(client, timeStampedRequest);
                    break;
                case LoneroStratumMethods.Submit:
                    await OnSubmitAsync(client, timeStampedRequest);
                    break;
                case LoneroStratumMethods.KeepAlive:
                    context.LastActivity = MasterClock.Now; // recognize activity.
                    break;
                default:
                    _logger.Debug($"[{client.ConnectionId}] Unsupported RPC request: {JsonConvert.SerializeObject(request, Globals.JsonSerializerSettings)}");
                    client.RespondError(StratumError.Other, $"Unsupported request {request.Method}", request.Id);
                    break;
            }
        }

        private void OnLogin(IStratumClient client, Timestamped<JsonRpcRequest> tsRequest)
        {
            var request = tsRequest.Value;
            var context = client.GetContextAs<LoneroWorkerContext>();

            if (request.Id == null)
            {
                client.RespondError(StratumError.MinusOne, "missing request id", request.Id);
                return;
            }

            var loginRequest = request.ParamsAs<LoneroLoginRequest>();

            if (string.IsNullOrEmpty(loginRequest?.Login))
            {
                client.RespondError(StratumError.MinusOne, "missing login", request.Id);
                return;
            }

            // extract worker & miner.
            var split = loginRequest.Login.Split('.');
            context.MinerName = split[0];
            context.WorkerName = split.Length > 1 ? split[1] : null;

            // set useragent.
            context.UserAgent = loginRequest.UserAgent;

            // set payment-id if any.
            var index = context.MinerName.IndexOf('#');
            if (index != -1)
            {
                context.MinerName = context.MinerName.Substring(0, index);
                context.PaymentId = context.MinerName.Substring(index + 1);
            }

            var hasValidAddress = ValidateAddress(context.MinerName);
            context.IsAuthorized = context.IsSubscribed = hasValidAddress;

            if (!context.IsAuthorized)
            {
                client.RespondError(StratumError.MinusOne, "invalid login", request.Id);
                return;
            }

            var loginResponse = new LoneroLoginResponse
            {
                Id = client.ConnectionId,
                Job = ((LoneroJobManager)PoolContext.JobManager).CurrentJob.CreateWorkerJob(client)
            };

            client.Respond(loginResponse, request.Id);

            // log association
            _logger.Information($"[{client.ConnectionId}] = {loginRequest.Login} = {client.RemoteEndpoint.Address}");

            // recognize activity.
            context.LastActivity = MasterClock.Now;
        }

        private void OnGetJob(IStratumClient client, Timestamped<JsonRpcRequest> tsRequest)
        {

        }

        private async Task OnSubmitAsync(IStratumClient client, Timestamped<JsonRpcRequest> tsRequest)
        {
        }

        private bool ValidateAddress(string address)
        {
            // check address length.
            if (address.Length != LoneroConstants.AddressLength[CoinType.LNR])
                return false;

            var addressPrefix = LibCryptonote.DecodeAddress(address);
            if (addressPrefix != _poolAddressBase58Prefix)
                return false;

            return true;
        } 
    }
}
