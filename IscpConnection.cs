using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// iSCPをインポート。
using iSCP;
using System;
using System.Threading.Tasks;
using System.Net.Http;
using iSCP.Model;
using iSCP.Transport;

using intdash.Api;
using intdash.Client;
using System.Threading;

public partial class IscpConnection : MonoBehaviour
{
    public static IscpConnection Shared { private set; get; }
    /// <summary>
    /// このフラグが true の場合 IscpConnection.Shared にセットされ、IscpConnection.GetOrCreateSharedInstance() で参照が可能になります。
    /// </summary>
    [SerializeField] private bool isShared = true;
    /// <summary>
    /// このコネクションを共有インスタンスとして設定するかどうか。
    /// 
    /// Awake より前にのみ実行可能です。
    /// </summary>
    public void SetSharedFlag(bool shared)
    {
        isShared = shared;
    }

    public enum ConnectionLogLevel
    {
        Debug = 1,
        Info = 2,
        Warning = 3,
        Error = 4,
        None = 5
    }
    public ConnectionLogLevel LogLevel = ConnectionLogLevel.Info;

    [SerializeField] private string ConnName = "Connection1";

    [SerializeField] private IntdashApiManager ApiManager;

    private HttpClient httpClient => ApiManager.HttpClient;
    private Configuration apiConfiguration;

    public static IscpConnection GetOrCreateSharedInstance()
    {
        if (Shared != null)
            return Shared;
        var instance = FindObjectOfType<IscpConnection>();
        if (instance != null)
            return instance;
        var obj = new GameObject(nameof(IscpConnection));
        var script = obj.AddComponent<IscpConnection>();
        script.isShared = true;
        return script;
    }

    private bool awaked = false;
    private bool isApplicationQuitting = false;

    private void Awake()
    {
        if (isShared && Shared != null)
        {
            Destroy(this);
            return;
        }
        Debug.Log($"[{ConnName}] Awake - IscpConnection");
        if (isShared)
        {
            Shared = this;
        }
        awaked = true;
        IscpLog.Shared.Level = IscpLogLevel.Debug;
        IscpLog.Shared.OnOutputLog += OnOutputLog;
    }

    private void OnOutputLog(string message, IscpLogLevel level, string function, string file, int line)
    {
        if (((int)level < (int)LogLevel)) { return; }
        var output = $"[{ConnName}] [iSCP] " + level.LogHeader() + " " + message;
        switch (level)
        {
            case IscpLogLevel.Error:
                Debug.LogError(output);
                break;
            case IscpLogLevel.Warning:
                Debug.LogWarning(output);
                break;
            default:
                Debug.Log(output);
                break;
        }
    }

    private void Start()
    {
        Debug.Log($"[{ConnName}] Start - IscpConnection");
        if (ApiManager == null)
        {
            ApiManager = IntdashApiManager.GetOrCreateSharedInstance();
        }
        if (ApiManager != null)
        {
            this.apiConfiguration = ApiManager.Configuration;
        }
        if (ConnectOnStart)
        {
            if (!ApiManager.IsEnableApi)
            {
                ApiManager.OnEnableApi += APIManager_OnEnableAPI;
            }
            else
            {
                this.Connect();
            }
        }
    }

    private void APIManager_OnEnableAPI(string version)
    {
        Debug.Log($"[{ConnName}] APIManager_OnEnableAPI() - IscpConnection");
        ApiManager.OnEnableApi -= APIManager_OnEnableAPI;
        this.Connect();
    }

    private void OnDestroy()
    {
        Debug.Log($"[{ConnName}] OnDestroy - IscpConnection");
        if (Shared == this)
            Shared = null;
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams) r.SetUpstream(null);
            this.registeredUpstreams.Clear();
        }
        lock (downstreamLock)
        {
            foreach (var r in registeredDownstreams) r.SetDownstream(null);
            this.registeredDownstreams.Clear();
        }
        if (awaked)
        {
            IscpLog.Shared.OnOutputLog -= OnOutputLog;
        }
        if (!isApplicationQuitting)
        {
            Close();
        }

    }

    private async void OnApplicationQuit()
    {
        Debug.Log($"[{ConnName}] OnApplicationQuit - IscpConnection");
        isApplicationQuitting = true;

        try
        {
            await CloseAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[{ConnName}] CloseAsync failed on application quit. {e.Message} - IscpConnection");
        }
    }
}


partial class IscpConnection : IConnectionCallbacks
{
    private string serverUrl;
    public string ServerUrl => serverUrl;
    private string accessToken;
    public string AccessToken => accessToken;

    [SerializeField]

    /// <summary>
    /// NodeID が空の状態であっても IntdashApiManager の AuthorizationType が ClientSecret であり、ClientID が設定されている場合はその情報が自動でセットされます。
    /// </summary>
    [IntdashLabel("Node ID (Edge UUID)")]
    public string NodeId = "";

    private string projectUuid;
    public string ProjectUuid => projectUuid;

    public bool ConnectOnStart = true;

    /// <summary>
    /// コネクションのタイムアウト時間（秒）。
    /// </summary>
    public uint ConnectionTimeout = 60;
    /// <summary>
    /// 送信メッセージの受信タイムアウト時間（秒）。
    /// `0` 以下の場合はタイムアウトしません。
    /// </summary>
    public int SendMessageResponseTimeout = 120;

    public Connection Connection { private set; get; }

    /// <summary>
    /// 接続中かどうか。
    /// </summary>
    public bool IsConnecting => Connection != null;

    private int isClosing = 0;
    internal bool IsClosing => Interlocked.CompareExchange(ref isClosing, 0, 0) == 1;

    public delegate void ConnectionDelegate(IscpConnection connection);
    public delegate void ConnectionWithErrorDelgate(IscpConnection connection, Exception error);
    /// <summary>
    /// コネクションがオープンされた際のイベントです。
    /// </summary>
    public event ConnectionDelegate OnConnectConnection;
    /// <summary>
    /// コネクションのオープンに失敗した際のイベントです。
    /// </summary>
    public event ConnectionWithErrorDelgate OnConnectFailWithErrorConnection;
    /// <summary>
    /// コネクションがクローズされた際のイベントです。
    /// </summary>
    public event ConnectionDelegate OnDisconnectConnection;
    /// <summary>
    /// コネクションが再オープンされた際のイベントです。
    /// </summary>
    public event ConnectionDelegate OnReconnectConnection;
    /// <summary>
    /// コネクション内でエラーが発生した際のイベントです。
    /// </summary>
    public event ConnectionWithErrorDelgate OnFailWithErrorConnection;

    public void Connect()
    {
        if (Connection != null) return;
        Debug.Log($"[{ConnName}] iSCP Connection connect. - IscpConnection");
        // 接続情報のセットアップをします。
        this.serverUrl = ApiManager.ServerUrl;
        this.projectUuid = ApiManager.ProjectUuid;
        this.accessToken = ApiManager.AccessToken;
        if (string.IsNullOrEmpty(NodeId) && ApiManager.Type == IntdashApiManager.AuthorizationType.EdgeClientSecret)
        {
            NodeId = ApiManager.EdgeClientSecretInfo.ClientId;
        }
        var urls = ServerUrl.Split(new string[] { "://" }, StringSplitOptions.None);
        string address;
        var enableTls = false;
        if (urls.Length == 1)
        {
            address = urls[0];
        }
        else
        {
            enableTls = urls[0] == "https";
            address = urls[1];
        }
        // WebSocketを使って接続するように指定します。
        IConnector connector;
        {
            var wsConnector = new WebSocketConnector(enableTls: enableTls);
            if (ApiManager.ClientAuthCertificate != null)
            {
                wsConnector.ClientAuthCertificate = ApiManager.ClientAuthCertificate;
            }
            connector = wsConnector;
        }
        Connection.Connect(
            address: address,
            connector: connector,
            tokenSource: (token) =>
            {
                // アクセス用のトークンをAPIManagerから取得します。接続時に発生するイベントにより使用されます。
                ApiManager.GetEnableToken(token);
            },
            nodeId: NodeId,
            projectUuid: ProjectUuid,
            connectTimeout: SendMessageResponseTimeout * 1000,
            pingTimeout: ConnectionTimeout,
            pingInterval: ConnectionTimeout,
            completion: (con, exception) =>
            {
                if (!(con is Connection connection))
                {
                    // 接続失敗。
                    Debug.LogError($"Failed to connect iSCP. {exception.Message} - IscpConnection");
                    this.OnConnectFailWithErrorConnection?.Invoke(this, exception);
                    return;
                }
                // 接続成功。
                Debug.Log($"iSCP Connection[{ConnName}] successfully opened! - IscpConnection");
                this.Connection = connection;
                connection.Callbacks = this; // IConnectionCallbacks
                this.OnConnectConnection?.Invoke(this);
                // 以降、StartUpstreamやStartDownstreamなどが実行可能になります。
                this.OpenUpstream();
                this.OpenDownstream();
            });
    }

    public void Close()
    {
        if (!(this.Connection is Connection connection)) { return; }
        if (Interlocked.Exchange(ref isClosing, 1) == 1)
        {
            // 既にクローズ処理中。
            return;
        }
        Debug.Log($"[{ConnName}] iSCP Connection close. - IscpConnection");

        this.Connection = null;
        Upstream[] upstreams;
        Upstream[] usedUpstreams;
        string measurementUuid;
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams) r.SetUpstream(null);
            upstreams = this.upstreams.ToArray();
            this.upstreams.Clear();
            usedUpstreams = this.usedUpstreams.ToArray();
            this.usedUpstreams.Clear();
            measurementUuid = this.SessionId;
        }
        lock (downstreamLock)
        {
            foreach (var r in registeredDownstreams) r.SetDownstream(null);
        }

        // 非同期処理
        Task.Run(async () =>
        {
            try
            {
                // アップストリームを閉じる。
                for (int i = 0; i < upstreams.Length; i++)
                {
                    var u = upstreams[i];
                    var closeSession = i == (upstreams.Length - 1);
                    Debug.Log($"Close upstream(id: {u.Id}, closeSession: {closeSession})");
                    var err = await u.CloseAsync(closeSession).ConfigureAwait(false);
                    if (err != null)
                    {
                        Debug.Log($"Failed to close upstream(id: {u.Id})");
                        continue;
                    }
                    Debug.Log($"Success to close upstream(id: {u.Id})");
                }
                // コネクションを閉じる。
                var exception = await connection.CloseAsync().ConfigureAwait(false);
                if (exception != null)
                {
                    Debug.LogError($"Failed to close connection[{ConnName}]. {exception.Message} - IscpConnection");
                }
                Debug.Log($"Success to close connection[{ConnName}]. - IscpConnection");
                connection.Dispose();

                try
                {
                    // 計測の終了処理。
                    await EndMeasurementAsync(usedUpstreams, measurementUuid).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Debug.LogError($"Failed to end measurement[{measurementUuid}]. {e.Message} - IscpConnection");
                }
            }
            finally
            {
                Interlocked.Exchange(ref isClosing, 0);
            }
        });
    }

    public async Task CloseAsync()
    {
        if (!(this.Connection is Connection connection)) { return; }
        if (Interlocked.Exchange(ref isClosing, 1) == 1)
        {
            // 既にクローズ処理中。
            return;
        }

        try
        {
            Debug.Log($"[{ConnName}] iSCP Connection close. - IscpConnection");
            this.Connection = null;
            Upstream[] upstreams;
            Upstream[] usedUpstreams;
            string measurementUuid;
            lock (upstreamLock)
            {
                foreach (var r in registeredUpstreams) r.SetUpstream(null);
                upstreams = this.upstreams.ToArray();
                this.upstreams.Clear();
                usedUpstreams = this.usedUpstreams.ToArray();
                this.usedUpstreams.Clear();
                measurementUuid = this.SessionId;
            }
            lock (downstreamLock)
            {
                foreach (var r in registeredDownstreams) r.SetDownstream(null);
            }

            // アップストリームを閉じる。
            for (int i = 0; i < upstreams.Length; i++)
            {
                var u = upstreams[i];
                var closeSession = i == (upstreams.Length - 1);
                Debug.Log($"Close upstream(id: {u.Id}, closeSession: {closeSession})");
                var err = await u.CloseAsync(closeSession).ConfigureAwait(false);
                if (err != null)
                {
                    Debug.Log($"Failed to close upstream(id: {u.Id})");
                    continue;
                }
                Debug.Log($"Success to close upstream(id: {u.Id})");
            }

            // コネクションを閉じる。
            var exception = await connection.CloseAsync().ConfigureAwait(false);
            if (exception != null)
            {
                Debug.LogError($"Failed to close connection[{ConnName}]. {exception.Message} - IscpConnection");
                return;
            }
            Debug.Log($"Success to close connection[{ConnName}]. - IscpConnection");
            connection.Dispose();

            try
            {
                // 計測の終了処理。
                await EndMeasurementAsync(usedUpstreams, measurementUuid).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Debug.LogError($"Failed to end measurement[{measurementUuid}]. {e.Message} - IscpConnection");
            }
        }
        finally
        {
            Interlocked.Exchange(ref isClosing, 0);
        }

    }
    public void OnDisconnect(Connection connection)
    {
        Debug.Log($"[{ConnName}] OnDisconnect - IscpConnection.IConnectionCallbacks");
        this.OnDisconnectConnection?.Invoke(this);
    }

    public void OnFailWithError(Connection connection, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError(error: {error.Message})  - IscpConnection.IConnectionCallbacks");
        this.OnFailWithErrorConnection?.Invoke(this, error);
    }

    public void OnReconnect(Connection connection)
    {
        Debug.Log($"[{ConnName}] OnReconnect - IscpConnection.IConnectionCallbacks");
        this.OnReconnectConnection?.Invoke(this);
    }
}

public interface IIscpDownstreamCallbacks
{
    void OnReceiveMetadata(IscpDownstream downstream, DownstreamMetadata message);
}

public class IscpDownstream : IEquatable<IscpDownstream>
{
    public readonly Guid Id;

    public readonly string NodeId;
    public readonly DataFilter DataFilter;
    internal Action<DateTime, DataPointGroup[]> Callback;

    internal Downstream Downstream { private set; get; }
    private object streamLock = new object();

    internal void SetDownstream(Downstream downstream)
    {
        lock (streamLock)
        {
            Downstream = downstream;
        }
    }

    public IIscpDownstreamCallbacks Callbacks;

    public DateTime? BaseTime { internal set; get; } = null;
    public byte? BaseTimePrioerity { internal set; get; } = null;

    public IscpDownstream(string nodeId, string dataName, string dataType, Action<DateTime, DataPointGroup[]> callback)
    {
        this.Id = Guid.NewGuid();

        this.NodeId = nodeId;
        this.DataFilter = new DataFilter(dataName, dataType);
        this.Callback = callback;
    }

    public static bool operator ==(IscpDownstream l, IscpDownstream r) => l?.Equals(r) ?? (r is null);
    public static bool operator !=(IscpDownstream l, IscpDownstream r) => !(l == r);

    public bool Equals(IscpDownstream other)
    {
        if (ReferenceEquals(other, null)) { return false; }
        if (this.Id != other.Id) { return false; }
        return true;
    }

    public override int GetHashCode()
    {
        int hash = 1;
        if (Id != Guid.Empty) hash ^= Id.GetHashCode();
        return hash;
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as IscpDownstream);
    }
}

partial class IscpConnection : IDownstreamCallbacks
{
    /// <summary>
    /// 受信したデータポイントをログに出力するかどうか。
    /// </summary>
    public bool EnableReceivedDataPointsLog = false;

    private object downstreamLock = new object();
    private List<IscpDownstream> registeredDownstreams = new List<IscpDownstream>();
    private List<IscpDownstream> GetRegisteredDownstreams(Downstream downstream)
    {
        var results = new List<IscpDownstream>();
        lock (downstreamLock)
        {
            foreach (var r in registeredDownstreams)
            {
                if (r.Downstream == downstream)
                {
                    results.Add(r);
                }
            }
        }
        return results;
    }

    /// <summary>
    /// ダウンストリームを登録します。
    /// </summary>
    /// <param name="nodeId">ノードID</param>
    /// <param name="dataName">データ名</param>
    /// <param name="dataType">データタイプ</param>
    /// <param name="callback">データポイント受信時のコールバック</param>
    public IscpDownstream RegisterDownstream(string nodeId, string dataName, string dataType, Action<DateTime, DataPointGroup[]> callback)
    {
        lock (downstreamLock)
        {
            Debug.Log($"[{ConnName}] Subscribe to iSCP. dataName: {dataName}, dataType: {dataType}, nodeId: {nodeId} - IscpConnection");
            var downstream = new IscpDownstream(nodeId, dataName, dataType, callback);
            registeredDownstreams.Add(downstream);
            return downstream;
        }
    }

    private class DownstreamRequest
    {
        public string NodeId;
        public DataFilter DataFilter;
        public List<IscpDownstream> Downstreams;

        public DownstreamRequest(string nodeId, DataFilter dataFilter)
        {
            NodeId = nodeId;
            DataFilter = dataFilter;
            Downstreams = new List<IscpDownstream>();
        }
    }

    private void OpenDownstream()
    {
        int downstreamCount;
        lock (downstreamLock)
        {
            downstreamCount = registeredDownstreams.Count;
            if (downstreamCount == 0) return;
        }
        Debug.Log($"[{ConnName}] OpenDownstream({downstreamCount} streams) - IscpConnection");

        var requests = new List<DownstreamRequest>();
        lock (downstreamLock)
        {
            foreach (var r in registeredDownstreams)
            {
                var request = requests.Find(v => v.NodeId == r.NodeId && v.DataFilter == r.DataFilter);
                if (request == null)
                {
                    request = new DownstreamRequest(r.NodeId, r.DataFilter);
                    requests.Add(request);
                }
                request.Downstreams.Add(r);
            }
        }
        Task.Run(async () =>
        {
            foreach (var r in requests)
            {
                var filter = new DownstreamFilter(
                    r.NodeId, // 送信元ノードのIDを指定します。
                    new DataFilter[] { r.DataFilter }
                );
                Debug.Log($"[{ConnName}] OpenDownstream name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId} - IscpConnection");
                // ダウンストリームをオープンします。
                var (downstream, exception) = await Connection.OpenDownstreamAsync(
                    downstreamFilters: new DownstreamFilter[] { filter },
                    omitEmptyChunk: true,
                    openTimeout: SendMessageResponseTimeout * 1000,
                    closeTimeout: SendMessageResponseTimeout * 1000).ConfigureAwait(false);
                if (downstream == null)
                {
                    // オープン失敗。
                    Debug.LogError($"[{ConnName}] Failed to open downstream. {exception?.Message ?? ""}, name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId} - IscpConnection");
                    return;
                }
                // オープン成功。
                Debug.Log($"[{ConnName}] Successfully open downstream(id: {downstream.Id}). name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId} - IscpConnection");
                // 受信データを取り扱うためにデリゲートを設定します。
                downstream.Callbacks = this; // IDownstreamCallbacks
                foreach (var d in r.Downstreams)
                {
                    d.SetDownstream(downstream);
                }
            }
        });
    }

    /// <summary>
    /// 登録したダウンストリームを解除します。
    /// </summary>
    public void UnregisterDownstream(IscpDownstream downstream)
    {
        lock (downstreamLock)
        {
            registeredDownstreams.Remove(downstream);
            downstream.SetDownstream(null);
        }
    }

    public void OnReceiveChunk(Downstream downstream, DownstreamChunk message)
    {
        if (message.DataPointGroups.Length == 0) return;
        if (EnableReceivedDataPointsLog)
            Debug.Log($"[{ConnName}] OnReceiveChunk downstream[{downstream.Id}], {message.DataPointGroups.Length} groups. - IscpConnection.IDownstreamCallbacks");
        if (IscpDownstreamRateCalculator.Shared is IscpDownstreamRateCalculator calc)
        {
            foreach (var group in message.DataPointGroups)
            {
                foreach (var point in group.DataPoints)
                {
                    calc.AddByte((ulong)point.Payload.Length);
                }
            }
        }

        foreach (var r in GetRegisteredDownstreams(downstream))
        {
            if (!(GetBaseTime(r, message.DataPointGroups) is DateTime baseTime))
            {
                Debug.LogWarning($"[{ConnName}] BaseTime is null for downstream[{downstream.Id}] - IscpConnection.IDownstreamCallbacks");
                continue;
            }
            r.Callback?.Invoke(baseTime, message.DataPointGroups);
        }
    }

    private DateTime? GetBaseTime(IscpDownstream downstream, DataPointGroup[] dataPointGroups)
    {
        if (downstream.BaseTime != null) return downstream.BaseTime;
        foreach (var group in dataPointGroups)
        {
            foreach (var dataPoint in group.DataPoints)
            {
                var baseTime = DateTime.UtcNow.AddTicks(-dataPoint.ElapsedTime);
                downstream.BaseTime = baseTime;
                return baseTime;
            }
        }
        return null;
    }

    public void OnReceiveMetadata(Downstream downstream, DownstreamMetadata message)
    {
        var rs = GetRegisteredDownstreams(downstream);
        switch (message.Type)
        {
            case DownstreamMetadata.MetadataType.BaseTime:
                var baseTime = message.BaseTime.Value;
                var startMeasurement = false;
                if (baseTime.Name == "api_first_received")
                {
                    startMeasurement = true;
                }
                var dateTime = baseTime.BaseTime_.ToDateTimeFromUnixTimeTicks();
                foreach (var r in rs)
                {
                    if (r.BaseTimePrioerity == null || (r.BaseTimePrioerity is byte priority && priority <= baseTime.Priority) || startMeasurement)
                    {
                        r.BaseTimePrioerity = baseTime.Priority;
                        r.BaseTime = dateTime;
                    }
                }
                Debug.Log($"[{ConnName}] OnReceiveMetadata downstream[{downstream.Id}] type: {message.Type} name: {baseTime.Name}, baseTime: {dateTime.ToLocalTime()}, priority: {baseTime.Priority} - IscpConnection.IDownstreamCallbacks");
                break;
            default: break;
        }

        foreach (var r in rs)
        {
            r.Callbacks?.OnReceiveMetadata(r, message);
        }
    }

    public void OnFailWithError(Downstream downstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError downstream[{downstream.Id}] - IscpConnection.IDownstreamCallbacks");
    }

    public void OnCloseWithError(Downstream downstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnCloseWithError downstream[{downstream.Id}] - IscpConnection.IDownstreamCallbacks");
        if (IsClosing)
        {
            Debug.Log($"[{ConnName}] Skip ReopenDownstream because closing. - IscpConnection");
            return;
        }

        this.Connection?.ReopenDownstream(
           downstream: downstream,
           completion: (newStream, error) =>
           {
               if (newStream == null || error != null)
               {
                   Debug.LogWarning($"[{ConnName}] ReopenDownstream failed. {error?.Message ?? ""} - IscpConnection");
                   return;
               }
               if (IsClosing)
               {
                   Debug.Log($"[{ConnName}] Close reopened downstream because closing. - IscpConnection");
                   newStream?.Close();
                   return;
               }
               var rs = GetRegisteredDownstreams(downstream);
               if (rs.Count == 0)
               {
                   Debug.LogWarning($"[{ConnName}] ReopenDownstream failed. Not found registered downstream[{downstream.Id}] - IscpConnection");
                   newStream.Close();
                   return;
               }
               lock (downstreamLock)
               {
                   newStream.Callbacks = this;
                   foreach (var r in rs)
                   {
                       r.SetDownstream(newStream);
                   }
                   Debug.Log($"[{ConnName}] ReopenDownstream successfully, new downstream[{newStream.Id}] - IscpConnection");
                   Task.Run(() =>
                   {
                       // 不要になったストリームを閉じる(解放する)
                       downstream.Close();
                   });
               }
           });
    }

    public void OnResume(Downstream downstream)
    {
        Debug.Log($"[{ConnName}] OnResume downstream[{downstream.Id}] - IscpConnection.IDownstreamCallbacks");
    }
}

public interface IIscpUpstreamCallbacks
{
    void OnOpen(IscpUpstream upstream, string sequenceId);
    void OnGenerateChunk(IscpUpstream upstream, string sequenceId, UpstreamChunk message);
    void OnReceiveAck(IscpUpstream upstream, string sequenceId, UpstreamChunkAck message);
}

public class IscpUpstream : IEquatable<IscpUpstream>
{
    public readonly Guid Id;

    public readonly bool Persist;
    public readonly FlushPolicy FlushPolicy;

    public readonly IscpConnection Connection;
    internal Upstream Upstream { private set; get; }

    public void SetUpstream(Upstream upstream)
    {
        Upstream = upstream;
        SequenceId = upstream?.Id.ToString() ?? "";
    }

    public bool IsOpen => Upstream != null;

    public IIscpUpstreamCallbacks Callbacks;

    public string SequenceId { private set; get; }

    public string SessionId { internal set; get; }

    internal IscpUpstream(IscpConnection connection, bool persist, FlushPolicy? flushPolicy = null)
    {
        Connection = connection;
        Id = Guid.NewGuid();
        Persist = persist;
        FlushPolicy = flushPolicy ?? FlushPolicy.IntervalOrBufferSize(50, 10_000);
    }

    public static bool operator ==(IscpUpstream l, IscpUpstream r) => l?.Equals(r) ?? (r is null);
    public static bool operator !=(IscpUpstream l, IscpUpstream r) => !(l == r);

    public bool Equals(IscpUpstream other)
    {
        if (ReferenceEquals(other, null)) { return false; }
        if (this.Id != other.Id) { return false; }
        return true;
    }

    public override int GetHashCode()
    {
        int hash = 1;
        if (Id != Guid.Empty) hash ^= Id.GetHashCode();
        return hash;
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as IscpUpstream);
    }

    #region SendDataPoint

    public Exception SendDataPoint(string dataName, string dataType, byte[] payload)
    {
        return SendDataPoint(dataName: dataName, dataType: dataType, dateTime: DateTime.UtcNow, payload: payload);
    }

    public Exception SendDataPoint(string dataName, string dataType, DateTime dateTime, byte[] payload)
    {
        var elapsedTime = dateTime.Ticks - Connection.EdgeRTCBaseTimeTicks;
        return SendDataPoint(dataName: dataName, dataType: dataType, elapsedTime: elapsedTime, payload: payload);
    }

    public Exception SendDataPoint(string dataName, string dataType, long elapsedTime, byte[] payload)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (!(Upstream is Upstream upstream)) return new Exception("Upstream not yet opened.");
        if (IscpUpstreamRateCalculator.Shared is IscpUpstreamRateCalculator calc)
        {
            calc.AddByte((ulong)payload.Length);
        }
        var dataId = new DataId(name: dataName, type: dataType);
        var dataPoint = new DataPoint(elapsedTime: elapsedTime, payload: payload);
        var error = upstream.WriteDataPoint(dataId, dataPoint);
        if (error != null)
        {
            lock (Connection.failedSendLock)
            {
                var buffer = new List<DataPointGroup>();
                if (Connection.FailedSendDataPoints.ContainsKey(upstream.Id))
                {
                    buffer = Connection.FailedSendDataPoints[upstream.Id];
                }
                buffer.Add(new DataPointGroup(dataId, new DataPoint[] { dataPoint }));
                Connection.FailedSendDataPoints[upstream.Id] = buffer;
            }
        }
        return error;
    }

    #endregion

    #region SendDataPoints

    public Exception SendDataPoints((string name, string type, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (!(Upstream is Upstream upstream)) return new Exception("Upstream not yet opened.");
        var now = DateTime.UtcNow.Ticks;
        if (IscpUpstreamRateCalculator.Shared is IscpUpstreamRateCalculator calc)
        {
            foreach (var point in points)
            {
                calc.AddByte((ulong)point.payload.Length);
            }
        }
        var groups = new List<DataPointGroup>();
        foreach (var p in points)
        {
            var dataId = new DataId(name: p.name, type: p.type);
            var elapsedTime = now - Connection.EdgeRTCBaseTimeTicks;
            var dataPoint = new DataPoint(elapsedTime: elapsedTime, payload: p.payload);
            groups.Add(new DataPointGroup(dataId, new DataPoint[] { dataPoint }));
        }
        return Connection.SendDataPoints(upstream, groups.ToArray());
    }

    public Exception SendDataPoints((string name, string type, DateTime dateTime, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (!(Upstream is Upstream upstream)) return new Exception("Upstream not yet opened.");
        if (IscpUpstreamRateCalculator.Shared is IscpUpstreamRateCalculator calc)
        {
            foreach (var point in points)
            {
                calc.AddByte((ulong)point.payload.Length);
            }
        }
        var groups = new List<DataPointGroup>();
        foreach (var p in points)
        {
            var dataId = new DataId(name: p.name, type: p.type);
            var elapsedTime = p.dateTime.Ticks - Connection.EdgeRTCBaseTimeTicks;
            var dataPoint = new DataPoint(elapsedTime: elapsedTime, payload: p.payload);
            groups.Add(new DataPointGroup(dataId, new DataPoint[] { dataPoint }));
        }
        return Connection.SendDataPoints(upstream, groups.ToArray());
    }

    public Exception SendDataPoints((string name, string type, long elapsedTime, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (!(Upstream is Upstream upstream)) return new Exception("Upstream not yet opened.");
        if (IscpUpstreamRateCalculator.Shared is IscpUpstreamRateCalculator calc)
        {
            foreach (var point in points)
            {
                calc.AddByte((ulong)point.payload.Length);
            }
        }
        var groups = new List<DataPointGroup>();
        foreach (var p in points)
        {
            var dataId = new DataId(name: p.name, type: p.type);
            var dataPoint = new DataPoint(elapsedTime: p.elapsedTime, payload: p.payload);
            groups.Add(new DataPointGroup(dataId, new DataPoint[] { dataPoint }));
        }
        return Connection.SendDataPoints(upstream, groups.ToArray());
    }

    #endregion
}

partial class IscpConnection : IUpstreamCallbacks
{
    /// <summary>
    /// 送信したデータポイントをログに出力するかどうか。
    /// </summary>
    public bool EnableSentDataPointsLog = false;

    private List<IscpUpstream> registeredUpstreams = new List<IscpUpstream>();
    private List<IscpUpstream> GetRegisteredUpstreams(Upstream upstream)
    {
        var result = new List<IscpUpstream>();
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams)
            {
                if (r.Upstream == upstream)
                {
                    result.Add(r);
                }
            }
        }
        return result;
    }

    private bool Persist
    {
        get
        {
            lock (upstreamLock)
            {
                foreach (var u in registeredUpstreams)
                {
                    if (u.Persist) return true;
                }
            }
            return false;
        }
    }

    private List<Upstream> upstreams = new List<Upstream>();
    private List<Upstream> usedUpstreams = new List<Upstream>();
    private object upstreamLock = new object();

    [SerializeField]
    [IntdashLabel("Session ID (Measurement UUID)")]
    private string SessionId = "";
    [SerializeField]
    private long generatedSequenceNumber = 0;
    [SerializeField]
    private long receivedSequenceNumber = 0;
    internal Dictionary<Guid, List<DataPointGroup>> FailedSendDataPoints = new Dictionary<Guid, List<DataPointGroup>>();
    internal object failedSendLock = new object();

    public long EdgeRTCBaseTimeTicks { private set; get; } = 0;

    public IscpUpstream RegisterUpstream(bool persist = false, FlushPolicy? flushPolicy = null)
    {
        var upstream = new IscpUpstream(this, persist, flushPolicy);
        lock (upstreamLock)
        {
            registeredUpstreams.Add(upstream);
        }
        return upstream;
    }

    public void UnregisterUpstream(IscpUpstream upstream)
    {
        lock (upstreamLock)
        {
            registeredUpstreams.Remove(upstream);
            upstream.SetUpstream(null);
        }
    }

    private void OpenUpstream()
    {
        int upstreamCount;
        lock (upstreamLock)
        {
            upstreamCount = registeredUpstreams.Count;
            if (upstreamCount <= 0) return;
        }
        Debug.Log($"[{ConnName}] OpenUpstream(nodeUuid: {NodeId ?? ""}, {upstreamCount} streams) - IscpConnection");
        if (string.IsNullOrEmpty(NodeId))
        {
            Debug.LogWarning($"[{ConnName}] Failed to open upstream. nodeUuid is null or empty. - IscpConnection");
            return;
        }
        var baseTime = DateTime.UtcNow;
        this.EdgeRTCBaseTimeTicks = baseTime.Ticks;
        Debug.Log($"baseTime: {baseTime.ToLocalTime().ToString("yyyy/MM/dd HH:mm:ss.ffffff")} - IscpConnection");
        // セッションIDを払い出します。
        if (!Persist)
        {
            InvokeOpenUpstream(baseTime);
            return;
        }
        Task.Run(async () =>
        {
            try
            {
                var measCreate = new intdash.Model.MeasCreate(basetime: baseTime.ToUniversalTime(), basetimeType: intdash.Model.MeasurementBaseTimeType.EdgeRtc, edgeUuid: NodeId);
                var api = new MeasMeasurementsApi(httpClient, apiConfiguration);
                string measId;
                if (string.IsNullOrEmpty(ProjectUuid))
                {
                    var data = await api.CreateMeasurementAsync(measCreate).ConfigureAwait(false);
                    measId = data.Uuid;
                }
                else
                {
                    var data = await api.CreateProjectMeasurementAsync(ProjectUuid, measCreate).ConfigureAwait(false);
                    measId = data.Uuid;
                }
                Debug.Log($"[{ConnName}] Create measurement successfully. measId: {measId} - IscpConnection");
                InvokeOpenUpstream(baseTime, measId);
            }
            catch (Exception e)
            {
                Debug.LogError($"[{ConnName}] Failed to create measurement. {e.Message} - IscpConnection");
                return;
            }
        });
    }

    private async void InvokeOpenUpstream(DateTime baseTime, string sessionId = "")
    {
        this.SessionId = sessionId;
        this.generatedSequenceNumber = 0;
        this.receivedSequenceNumber = 0;
        lock (failedSendLock)
        {
            FailedSendDataPoints.Clear();
        }

        IscpUpstream[] upstreams;
        lock (upstreamLock)
        {
            upstreams = registeredUpstreams.ToArray();
        }

        foreach (var r in upstreams)
        {
            // Upstreamをオープンします。
            var persist = r.Persist && !string.IsNullOrEmpty(sessionId);
            var (upstream, exception) = await Connection.OpenUpstreamAsync(
                sessionId: r.Persist ? sessionId : "",
                persist: persist,
                flushPolicy: r.FlushPolicy,
                openTimeout: SendMessageResponseTimeout * 1000,
                closeTimeout: SendMessageResponseTimeout * 1000).ConfigureAwait(false);
            if (upstream == null)
            {
                // オープン失敗。
                Debug.LogError($"[{ConnName}] Failed to open upstream. {exception?.Message ?? ""} - IscpConnection");
                continue;
            }
            // オープン成功。
            Debug.Log($"[{ConnName}] Successfully open upstream(id: {upstream.Id}) - IscpConnection");
            // 送信するデータポイントを保存したい場合や、アップストリームのエラーをハンドリングしたい場合はコールバックを設定します。
            upstream.Callbacks = this; // IUpstreamCallbacks
            lock (upstreamLock)
            {
                this.upstreams.Add(upstream);
                this.usedUpstreams.Add(upstream);
                r.SetUpstream(upstream);
                r.SessionId = upstream.SessionId;
            }

            // Send first data
            var metadata = new BaseTime(
                sessionId: upstream.SessionId,
                name: "edge_rtc",
                priority: 20,
                elapsedTime: 0,
                baseTime: baseTime.ToUnixTimeTicks()); // 基準時刻はUNIX時刻で送信します。
            var error = await Connection.SendBaseTimeAsync(
                metadata,
                persist: persist,
                sendTimeout: SendMessageResponseTimeout * 1000).ConfigureAwait(false);
            if (error != null)
            {
                Debug.LogWarning($"[{ConnName}] Failed to send baseTime: {error.Message}, upstreamId: {upstream.Id}, sessionId: {upstream.SessionId} - IscpConnection");
            }
            else
            {
                Debug.Log($"[{ConnName}] Success to send baseTime. upstreamId: {upstream.Id}, sessionId: {upstream.SessionId} - IscpConnection");
            }
            r.Callbacks?.OnOpen(r, r.SequenceId);
        }
    }

    public Exception SendDataPoints(Upstream upstream, DataPointGroup[] groups)
    {
        var error = upstream.WriteDataPoints(groups);
        if (error != null)
        {
            lock (failedSendLock)
            {
                var buffer = new List<DataPointGroup>();
                if (FailedSendDataPoints.ContainsKey(upstream.Id))
                {
                    buffer = FailedSendDataPoints[upstream.Id];
                }
                buffer.AddRange(groups);
                FailedSendDataPoints[upstream.Id] = buffer;
            }
        }
        return error;
    }

    private async Task EndMeasurementAsync(Upstream[] upstreams, string measurementUuid)
    {
        var generatedSequences = Interlocked.Read(ref this.generatedSequenceNumber);
        var receivedSequences = Interlocked.Read(ref this.receivedSequenceNumber);
        if (string.IsNullOrEmpty(measurementUuid)) return;
        Debug.Log($"EndMeasurement({upstreams.Length} upstreams, measurementUuid: {measurementUuid}) - IscpConnection");
        if (apiConfiguration == null) return;
        if (generatedSequences == receivedSequences)
        {
            Debug.Log($"RequestCompleteMeasurementAsync measurementUuid: {measurementUuid} - IscpConnection");
            try
            {
                var api = new MeasMeasurementsApi(httpClient, apiConfiguration);
                if (string.IsNullOrEmpty(ProjectUuid))
                {
                    var data = await api.CompleteMeasurementAsync(measurementUuid: measurementUuid.ToLower()).ConfigureAwait(false);
                }
                else
                {
                    var data = await api.CompleteProjectMeasurementAsync(ProjectUuid, measurementUuid: measurementUuid.ToLower()).ConfigureAwait(false);
                }
                Debug.Log("RequestCompleteMeasurementAsync successfully. - IscpConnection");
            }
            catch (Exception e)
            {
                Debug.Log($"RequestCompleteMeasurementAsync error. {e.Message} - IscpConnection");
                return;
            }
        }
        else
        {
            Debug.Log($"dropped measurement data. measurementUuid: {measurementUuid}, generatedSequences: {generatedSequences}, receveidSequences: {receivedSequences} - IscpConnection");
            foreach (var u in upstreams)
            {
                if (!u.Persist) continue;
                var state = u.GetState();
                var sequenceUuid = u.Id.ToString();
                var expectedDataPoints = state.TotalDataPoints;
                var finalSequenceNumber = state.LastIssuedSequenceNumber;
                Debug.Log($"RequestUpdateMeasurementSequenceAsync sequenceUuid: {sequenceUuid}, measurementUuid: {measurementUuid}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber} - IscpConnection");
                try
                {
                    var replace = new intdash.Model.MeasurementSequenceGroupReplace(
                        expectedDataPoints: (int)expectedDataPoints,
                        finalSequenceNumber: (int)finalSequenceNumber);
                    var api = new MeasMeasurementsApi(httpClient, apiConfiguration);
                    if (string.IsNullOrEmpty(ProjectUuid))
                    {
                        var data = await api.UpdateMeasurementSequenceAsync(
                            measurementUuid: measurementUuid.ToLower(),
                            sequencesUuid: sequenceUuid.ToLower(),
                            measurementSequenceGroupReplace: replace).ConfigureAwait(false);
                    }
                    else
                    {
                        var data = await api.UpdateProjectMeasurementSequenceAsync(
                            projectUuid: ProjectUuid,
                            measurementUuid: measurementUuid.ToLower(),
                            sequencesUuid: sequenceUuid.ToLower(),
                            measurementSequenceGroupReplace: replace).ConfigureAwait(false);
                    }
                    Debug.Log($"RequestUpdateMeasurementSequenceAsync successfully. streamId: {u.Id}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber} - IscpConnection");
                }
                catch (Exception e)
                {
                    Debug.Log($"RequestUpdateMeasurementSequenceAsync error. {e.Message}, streamId: {u.Id}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber} - IscpConnection");
                    return;
                }
            }
            {
                Debug.Log($"RequestEndMeasurementAsync measurementUuid: {measurementUuid} - IscpConnection");
                try
                {
                    var api = new MeasMeasurementsApi(httpClient, apiConfiguration);
                    if (string.IsNullOrEmpty(ProjectUuid))
                    {
                        var data = await api.EndMeasurementAsync(measurementUuid: measurementUuid.ToLower()).ConfigureAwait(false);
                    }
                    else
                    {
                        var data = await api.EndProjectMeasurementAsync(projectUuid: ProjectUuid, measurementUuid: measurementUuid.ToLower()).ConfigureAwait(false);
                    }
                    Debug.Log("RequestEndMeasurementAsync successfully. - IscpConnection");
                }
                catch (Exception e)
                {
                    Debug.Log($"RequestEndMeasurementAsync error. {e.Message} - IscpConnection");
                }
            }
        }
    }

    public void OnGenerateChunk(Upstream upstream, UpstreamChunk message)
    {
        if (EnableSentDataPointsLog)
            Debug.Log($"[{ConnName}] OnGenerateChunk upstream[{upstream.Id}], SequenceNumber: {message.SequenceNumber}, DataPointCount: {message.DataPointCount}, PayloadSize: {message.PayloadSize} - IscpConnection.IUpstreamCallbacks");
        Interlocked.Increment(ref generatedSequenceNumber);
        foreach (var r in GetRegisteredUpstreams(upstream)) 
        {
            r.Callbacks?.OnGenerateChunk(r, r.SequenceId, message);
        }
    }

    public void OnReceiveAck(Upstream upstream, UpstreamChunkAck message)
    {
        if (EnableSentDataPointsLog)
            Debug.Log($"[{ConnName}] OnReceiveAck upstream[{upstream.Id}], SequenceNumber: {message.SequenceNumber}, ResultCode: {message.ResultCode}, ResultString: {message.ResultString} - IscpConnection.IUpstreamCallbacks");
        Interlocked.Increment(ref receivedSequenceNumber);
        foreach (var r in GetRegisteredUpstreams(upstream))
        {
            r.Callbacks?.OnReceiveAck(r, r.SequenceId, message);
        }
    }

    public void OnFailWithError(Upstream upstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError upstream[{upstream.Id}], error: {error} - IscpConnection.IUpstreamCallbacks");
    }

    public void OnCloseWithError(Upstream upstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnCloseWithError upstream[{upstream.Id}], error: {error} - IscpConnection.IUpstreamCallbacks");
        if (IsClosing)
        {
            Debug.Log($"[{ConnName}] Skip ReopenUpstream because closing. - IscpConnection");
            return;
        }

        this.Connection?.ReopenUpstream(
            upstream: upstream,
            completion: (newStream, error) =>
            {
                if (newStream == null || error != null)
                {
                    Debug.LogWarning($"[{ConnName}] ReopenUpstream failed. {error?.Message ?? ""} - IscpConnection");
                    return;
                }
                if (IsClosing)
                {
                    Debug.Log($"[{ConnName}] Close reopened upstream because closing. - IscpConnection");
                    newStream?.Close();
                    return;
                }
                var rs = GetRegisteredUpstreams(upstream);
                if (rs.Count == 0)
                {
                    Debug.LogWarning($"[{ConnName}] ReopenUpstream failed. Not found registered upstream[{upstream.Id}] - IscpConnection");
                    newStream.Close();
                    return;
                }
                DataPointGroup[] retryGroups = null;
                lock (failedSendLock)
                {
                    if (FailedSendDataPoints.TryGetValue(upstream.Id, out var groups) &&
                    groups != null && groups.Count > 0)
                    {
                        retryGroups = groups.ToArray();
                        groups.Clear();
                        FailedSendDataPoints.Remove(upstream.Id);
                    }
                }
                lock (upstreamLock)
                {
                    newStream.Callbacks = this;
                    this.upstreams.Add(newStream);
                    this.usedUpstreams.Add(newStream);
                    foreach (var r in rs)
                    {
                        r.SetUpstream(newStream);
                    }
                    Debug.Log($"[{ConnName}] ReopenUpstream successfully, new upstream[{newStream.Id}] - IscpConnection");
                    // 未送信のデータを持っていれば送信する
                    if (retryGroups != null && retryGroups.Length > 0)
                    {
                        SendDataPoints(newStream, retryGroups);
                    }
                    Task.Run(() =>
                    {
                        // 不要になったストリームを閉じる(解放する)
                        upstream.Close();
                        lock (upstreamLock)
                        {
                            this.upstreams.Remove(upstream);
                        }
                    });
                }
                foreach (var r in rs)
                {
                    r.Callbacks?.OnOpen(r, r.SequenceId);
                }
            });
    }

    public void OnResume(Upstream upstream)
    {
        Debug.Log($"[{ConnName}] OnResume upstream[{upstream.Id}] - IscpConnection.IUpstreamCallbacks");
    }
}