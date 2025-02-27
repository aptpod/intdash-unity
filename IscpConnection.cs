using System.Collections;
using System.Collections.Generic;
using UnityEngine;

// iSCP���C���|�[�g�B
using iSCP;
using System;
using System.Threading.Tasks;
using System.Net.Http;
using iSCP.Model;
using iSCP.Transport;
using iSCP.Helpers;

using intdash.Api;
using intdash.Client;

public partial class IscpConnection : MonoBehaviour
{
    public static IscpConnection Shared { private set; get; }
    /// <summary>
    /// ���̃t���O�� true �̏ꍇ IscpConnection.Shared �ɃZ�b�g����AIscpConnection.GetOrCreateSharedInstance() �ŎQ�Ƃ��\�ɂȂ�܂��B
    /// </summary>
    [SerializeField] private bool isShared = true;

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
    private HttpClient httpClient;
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
            this.httpClient = ApiManager.HttpClient;
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
            this.registeredUpstreams.Clear();
        lock (downstreamLock)
            this.registeredDownstreams.Clear();
        this.Close();
        if (awaked)
        {
            IscpLog.Shared.OnOutputLog -= OnOutputLog;
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
    /// NodeID ����̏�Ԃł����Ă� IntdashApiManager �� AuthorizationType �� ClientSecret �ł���AClientID ���ݒ肳��Ă���ꍇ�͂��̏�񂪎����ŃZ�b�g����܂��B
    /// </summary>
    [IntdashLabel("Node ID (Edge UUID)")]
    public string NodeId = "";

    private string projectUuid;
    public string ProjectUuid => projectUuid;

    public bool ConnectOnStart = true;

    [SerializeField]
    private uint connectionTimeout = 30;

    public Connection Connection { private set; get; }

    /// <summary>
    /// �ڑ������ǂ����B
    /// </summary>
    public bool IsConnecting => Connection != null;

    public delegate void ConnectionDelegate(IscpConnection connection);
    public delegate void ConnectionWithErrorDelgate(IscpConnection connection, Exception error);
    /// <summary>
    /// �R�l�N�V�������I�[�v�����ꂽ�ۂ̃C�x���g�ł��B
    /// </summary>
    public event ConnectionDelegate OnConnectConnection;
    /// <summary>
    /// �R�l�N�V�����̃I�[�v���Ɏ��s�����ۂ̃C�x���g�ł��B
    /// </summary>
    public event ConnectionWithErrorDelgate OnConnectFailWithErrorConnection;
    /// <summary>
    /// �R�l�N�V�������N���[�Y���ꂽ�ۂ̃C�x���g�ł��B
    /// </summary>
    public event ConnectionDelegate OnDisconnectConnection;
    /// <summary>
    /// �R�l�N�V�������ăI�[�v�����ꂽ�ۂ̃C�x���g�ł��B
    /// </summary>
    public event ConnectionDelegate OnReconnectConnection;
    /// <summary>
    /// �R�l�N�V�������ŃG���[�����������ۂ̃C�x���g�ł��B
    /// </summary>
    public event ConnectionWithErrorDelgate OnFailWithErrorConnection;

    public void Connect()
    {
        if (Connection != null) return;
        Debug.Log($"[{ConnName}] iSCP Connection connect.");
        // �ڑ����̃Z�b�g�A�b�v�����܂��B
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
        // WebSocket���g���Đڑ�����悤�Ɏw�肵�܂��B
        ITransportConfig transportConfig = new WebSocket.Config(enableTls: enableTls);
        Connection.Connect(
            address: address,
            transportConfig: transportConfig,
            tokenSource: (token) =>
            {
                // �A�N�Z�X�p�̃g�[�N����APIManager����擾���܂��B�ڑ����ɔ�������C�x���g�ɂ��g�p����܂��B
                ApiManager.GetEnableToken(token);
            },
            nodeId: NodeId,
            projectUuid: ProjectUuid,
            pingTimeout: connectionTimeout,
            completion: (con, exception) =>
            {
                if (!(con is Connection connection))
                {
                    // �ڑ����s�B
                    Debug.LogError("Failed to connect iSCP. " + exception.Message);
                    this.OnConnectFailWithErrorConnection?.Invoke(this, exception);
                    return;
                }
                // �ڑ������B
                Debug.Log($"iSCP Connection[{ConnName}] successfully opened!");
                this.Connection = connection;
                connection.Callbacks = this; // IConnectionCallbacks
                this.OnConnectConnection?.Invoke(this);
                // �ȍ~�AStartUpstream��StartDownstream�Ȃǂ����s�\�ɂȂ�܂��B
                this.OpenUpstream();
                this.OpenDownstream();
            });
    }

    public void Close()
    {
        if (!(this.Connection is Connection connection)) { return; }
        this.Connection = null;
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams) r.SetUpstream(null);
            registeredUpstreams.Clear();
        }
        lock (downstreamLock)
            foreach (var r in registeredDownstreams) r.SetDownstream(null);
        var upstreams = this.usedUpstreams.ToArray();
        this.usedUpstreams.Clear();
        this.downstream = null;

        connection.Close((exception) =>
        {
            try
            {
                if (exception != null)
                {
                    Debug.LogError($"Failed to disconnect connection[{ConnName}]. {exception.Message}");
                    return;
                }
                Debug.Log($"Success to disconnect connection[{ConnName}].");
                connection.Dispose();
            }
            finally
            {
                EndUpstream(upstreams);
            }
        });
    }

    public async void CloseAsync()
    {
        if (!(this.Connection is Connection connection)) { return; }
        this.Connection = null;
        var upstreams = this.usedUpstreams.ToArray();
        this.usedUpstreams.Clear();
        this.downstream = null;
        var exception = await connection.CloseAsync().ConfigureAwait(false);
        if (exception != null)
        {
            Debug.LogError($"Failed to disconnect connection[{ConnName}]. {exception.Message}");
            return;
        }
        Debug.Log($"Success to disconnect connection[{ConnName}].");
        connection.Dispose();
        EndUpstream(upstreams);
    }
    public void OnDisconnect(Connection connection)
    {
        Debug.Log($"[{ConnName}] OnDisconnect - IConnectionCallbacks");
        this.OnDisconnectConnection?.Invoke(this);
    }

    public void OnFailWithError(Connection connection, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError(error: {error.Message})  - IConnectionCallbacks");
        this.OnFailWithErrorConnection?.Invoke(this, error);
    }

    public void OnReconnect(Connection connection)
    {
        Debug.Log($"[{ConnName}] OnReconnect - IConnectionCallbacks");
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
    internal Action<DateTime, DataPointGroup> Callback;

    internal Downstream Downstream { private set; get; }
    private object streamLock = new object();

    internal void SetDownstream(Downstream downstream)
    {
        lock (streamLock)
            Downstream = downstream;
    }

    public IIscpDownstreamCallbacks Callbacks;

    public DateTime BaseTime { internal set; get; }

    public IscpDownstream(string nodeId, string dataName, string dataType, Action<DateTime, DataPointGroup> callback)
    {
        this.Id = Guid.NewGuid();

        this.NodeId = nodeId;
        this.DataFilter = new DataFilter(dataName, dataType);
        this.Callback = callback;
        this.BaseTime = DateTime.Now;
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
    public bool EnableReceivedDataPointsLog = false;
    private object downstreamLock = new object();
    private Downstream downstream;

    private List<IscpDownstream> registeredDownstreams = new List<IscpDownstream>();

    /// <summary>
    /// �_�E���X�g���[����o�^���܂��B
    /// </summary>
    /// <param name="nodeId">�m�[�hID</param>
    /// <param name="dataName">�f�[�^��</param>
    /// <param name="dataType">�f�[�^�^�C�v</param>
    /// <param name="callback">�f�[�^�|�C���g��M���̃R�[���o�b�N</param>
    public IscpDownstream RegisterDownstream(string nodeId, string dataName, string dataType, Action<DateTime, DataPointGroup> callback)
    {
        lock (downstreamLock)
        {
            Debug.Log($"[{ConnName}] Subscribe to iSCP. dataName: {dataName}, dataType: {dataType}, nodeId: {nodeId}");
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
        if (registeredDownstreams.Count == 0) return;
        Debug.Log($"[{ConnName}] OpenDownstream({registeredDownstreams.Count} streams)");

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
                    r.NodeId, // ���M���m�[�h��ID���w�肵�܂��B
                    new DataFilter[] { r.DataFilter }
                );
                Debug.Log($"[{ConnName}] OpenDownstream name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId}");
                // �_�E���X�g���[�����I�[�v�����܂��B
                var (downstream, exception) = await Connection?.OpenDownstreamAsync(
                    downstreamFilters: new DownstreamFilter[] { filter },
                    omitEmptyChunk: true);
                if (downstream == null)
                {
                    // �I�[�v�����s�B
                    Debug.LogError($"[{ConnName}] Failed to open downstream. name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId}");
                    return;
                }
                // �I�[�v�������B
                Debug.Log($"[{ConnName}] Successfully open downstream(id: {downstream.Id}). name: {r.DataFilter.Name}, type: {r.DataFilter.Type}, nodeId: {r.NodeId}");
                this.downstream = downstream;
                // ��M�f�[�^����舵�����߂Ƀf���Q�[�g��ݒ肵�܂��B
                downstream.Callbacks = this; // IDownstreamCallbacks
                foreach (var d in r.Downstreams)
                {
                    d.SetDownstream(downstream);
                    d.BaseTime = DateTime.UtcNow;
                }
            }
        });
    }

    /// <summary>
    /// �o�^�����_�E���X�g���[�����������܂��B
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
            Debug.Log($"[{ConnName}] OnReceiveChunk downstream[{downstream.Id}], {message.DataPointGroups.Length} groups. - IDownstreamCallbacks");
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
        foreach (var r in this.registeredDownstreams)
        {
            if (r.Downstream != downstream) continue;
            foreach (var g in message.DataPointGroups)
            {
                r.Callback?.Invoke(r.BaseTime, g);
            }
        }
    }

    public void OnReceiveMetadata(Downstream downstream, DownstreamMetadata message)
    {
        switch (message.Type)
        {
            case DownstreamMetadata.MetadataType.BaseTime:
                var baseTime = message.BaseTime.Value;
                var dateTime = baseTime.BaseTime_.ToDateTimeFromUnixTimeTicks().ToLocalTime();
                foreach (var r in registeredDownstreams)
                {
                    if (downstream == r.Downstream) {
                        r.BaseTime = dateTime;
                    }
                }
                Debug.Log($"[{ConnName}] OnReceiveMetadata downstream[{downstream.Id}] type: {message.Type} name: {baseTime.Name}, baseTime: {dateTime} - IDownstreamCallbacks");
                break;
            default: break;
        }
        foreach (var r in registeredDownstreams)
        {
            if (r.Downstream != downstream) continue;
            r.Callbacks?.OnReceiveMetadata(r, message);
        }
    }

    public void OnFailWithError(Downstream downstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError downstream[{downstream.Id}] - IDownstreamCallbacks");
    }

    public void OnCloseWithError(Downstream downstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnCloseWithError downstream[{downstream.Id}] - IDownstreamCallbacks");
        this.Connection?.ReopenDownstream(
           downstream: downstream,
           completion: (newStream, error) =>
           {
               if (newStream == null || error != null)
               {
                   Debug.LogWarning($"[{ConnName}] ReopenDownstream failed. {error?.Message ?? ""}");
                   return;
               }
               lock (downstreamLock)
               {
                   this.downstream = newStream;
                   newStream.Callbacks = this;
                   foreach (var r in registeredDownstreams)
                   {
                       if (r.Downstream != downstream) continue;
                       r.SetDownstream(newStream);
                   }
                   Debug.Log($"[{ConnName}] ReopenDownstream successfully, new downstream[{newStream.Id}]");
                   Task.Run(() =>
                   {
                       // �s�v�ɂȂ����X�g���[�������(�������)
                       downstream.Close();
                   });
               }
           });
    }

    public void OnResume(Downstream downstream)
    {
        Debug.Log($"[{ConnName}] OnResume downstream[{downstream.Id}] - IDownstreamCallbacks");
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
    private object streamLock = new object();

    public void SetUpstream(Upstream upstream)
    {
        lock(streamLock)
            Upstream = upstream;
        if (upstream != null)
        {
            SequenceId = upstream.Id.ToString();
        }
    }

    public bool IsOpen => Upstream != null;

    public IIscpUpstreamCallbacks Callbacks;

    public string SequenceId { internal set; get; }

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
        if (Upstream == null) return new Exception("Upstream not yet opened.");
        if (IscpUpstreamRateCalculator.Shared is IscpUpstreamRateCalculator calc)
        {
            calc.AddByte((ulong)payload.Length);
        }
        var dataId = new DataId(name: dataName, type: dataType);
        var dataPoint = new DataPoint(elapsedTime: elapsedTime, payload: payload);
        lock (streamLock)
        {
            var error = Upstream?.WriteDataPoint(dataId, dataPoint);
            if (error != null)
            {
                var buffer = new List<DataPointGroup>();
                if (Connection.FailedSendDataPoints.ContainsKey(Upstream.Id))
                {
                    buffer = Connection.FailedSendDataPoints[Upstream.Id];
                }
                buffer.Add(new DataPointGroup(dataId, new DataPoint[] { dataPoint }));
                Connection.FailedSendDataPoints[Upstream.Id] = buffer;
            }
            return error;
        }
    }

    #endregion

    #region SendDataPoints

    public Exception SendDataPoints((string name, string type, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (Upstream == null) return new Exception("Upstream not yet opened.");
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
        return SendDataPoints(groups.ToArray());
    }

    public Exception SendDataPoints((string name, string type, DateTime dateTime, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (Upstream == null) return new Exception("Upstream not yet opened.");
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
        return SendDataPoints(groups.ToArray());
    }

    public Exception SendDataPoints((string name, string type, long elapsedTime, byte[] payload)[] points)
    {
        if (Connection == null) return new Exception("Connection is null");
        if (Upstream == null) return new Exception("Upstream not yet opened.");
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
        return SendDataPoints(groups.ToArray());
    }

    public Exception SendDataPoints(DataPointGroup[] groups)
    {
        lock (streamLock)
        {
            var error = Upstream?.WriteDataPoints(groups);
            if (error != null)
            {
                var buffer = new List<DataPointGroup>();
                if (Connection.FailedSendDataPoints.ContainsKey(Upstream.Id))
                {
                    buffer = Connection.FailedSendDataPoints[Upstream.Id];
                }
                buffer.AddRange(groups);
                Connection.FailedSendDataPoints[Upstream.Id] = buffer;
            }
            return error;
        }
    }

    #endregion
}

partial class IscpConnection : IUpstreamCallbacks
{
    public bool EnableSentDataPointsLog = false;

    private List<IscpUpstream> registeredUpstreams = new List<IscpUpstream>();

    private bool Persist
    {
        get
        {
            foreach (var u in registeredUpstreams)
            {
                if (u.Persist) return true;
            }
            return false;
        }
    }

    private List<Upstream> usedUpstreams = new List<Upstream>();
    private object upstreamLock = new object();

    [SerializeField]
    [IntdashLabel("Session ID (Measurement UUID)")]
    private string SessionId = "";
    [SerializeField]
    private UInt64 generatedSequenceNumber = 0;
    [SerializeField]
    private UInt64 receivedSequenceNumber = 0;
    internal Dictionary<Guid, List<DataPointGroup>> FailedSendDataPoints = new Dictionary<Guid, List<DataPointGroup>>();

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
        if (registeredUpstreams.Count <= 0) return;
        Debug.Log($"[{ConnName}] OpenUpstream(nodeUuid: {NodeId ?? ""})");
        if (string.IsNullOrEmpty(NodeId))
        {
            Debug.LogWarning($"[{ConnName}] Failed to open upstream. nodeUuid is null or empty.");
            return;
        }
        var baseTime = DateTime.UtcNow;
        this.EdgeRTCBaseTimeTicks = baseTime.Ticks;
        Debug.Log($"baseTime: {baseTime.ToLocalTime().ToString("yyyy/MM/dd HH:mm:ss.ffffff")}");
        // �Z�b�V����ID�𕥂��o���܂��B
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
                Debug.Log($"[{ConnName}] Create measurement successfully. measId: {measId}");
                InvokeOpenUpstream(baseTime, measId);
            }
            catch (Exception e)
            {
                Debug.LogError($"[{ConnName}] Failed to create measurement. {e.Message}");
                return;
            }
        });
    }

    private async void InvokeOpenUpstream(DateTime baseTime, string sessionId = "")
    {
        this.SessionId = sessionId;
        this.generatedSequenceNumber = 0;
        this.receivedSequenceNumber = 0;

        foreach (var r in registeredUpstreams)
        {
            // Upstream���I�[�v�����܂��B
            var persist = r.Persist && !string.IsNullOrEmpty(sessionId);
            var (upstream, exception) = await Connection.OpenUpstreamAsync(
                sessionId: r.Persist ? sessionId : "",
                persist: persist,
                flushPolicy: r.FlushPolicy);
            if (upstream == null)
            {
                // �I�[�v�����s�B
                Debug.LogError($"[{ConnName}] Failed to open upstream. {exception?.Message ?? ""}");
                continue;
            }
            // �I�[�v�������B
            Debug.Log($"[{ConnName}] Successfully open upstream(id: {upstream.Id})");
            // ���M����f�[�^�|�C���g��ۑ��������ꍇ��A�A�b�v�X�g���[���̃G���[���n���h�����O�������ꍇ�̓R�[���o�b�N��ݒ肵�܂��B
            upstream.Callbacks = this; // IUpstreamCallbacks
            this.usedUpstreams.Add(upstream);
            r.SetUpstream(upstream);
            r.SessionId = upstream.SessionId;

            // Send first data
            var metadata = new BaseTime(
                sessionId: upstream.SessionId,
                name: "edge_rtc",
                priority: 20,
                elapsedTime: 0,
                baseTime: baseTime.ToUnixTimeTicks()); // �������UNIX�����ő��M���܂��B
            var error = await Connection.SendBaseTimeAsync(metadata, persist);
            if (error != null)
            {
                Debug.LogWarning($"[{ConnName}] Failed to send baseTime: {error.Message}, upstreamId: {upstream.Id}, sessionId: {upstream.SessionId}");
                continue;
            }
            Debug.Log($"[{ConnName}] Success to send baseTime. upstreamId: {upstream.Id}, sessionId: {upstream.SessionId}");

            r.Callbacks?.OnOpen(r, r.SequenceId);
        }
    }

    private void EndUpstream(Upstream[] upstreams)
    {
        Debug.Log($"EndUpstream({upstreams.Length} upstreams, sessionId: {this.SessionId})");
        var measurementUuid = this.SessionId;
        var generatedSequences = this.generatedSequenceNumber;
        var receivedSequences = this.receivedSequenceNumber;
        if (string.IsNullOrEmpty(this.SessionId)) return;
        if (apiConfiguration == null) return;
        Task.Run(async () =>
        {
            if (generatedSequences == receivedSequences)
            {
                Debug.Log($"RequestCompleteMeasurementAsync measurementUuid: {measurementUuid}");
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
                    Debug.Log("RequestCompleteMeasurementAsync successfully.");
                }
                catch (Exception e)
                {
                    Debug.Log($"RequestCompleteMeasurementAsync error. {e.Message}");
                    return;
                }
            }
            else
            {
                Debug.Log($"dropped measurement data. measurmentId: {measurementUuid}, generatedSequences: {generatedSequences}, receveidSequences: {receivedSequences}");
                foreach (var u in upstreams)
                {
                    if (!u.Persist) continue;
                    var state = u.GetState();
                    var sequenceUuid = u.Id.ToString();
                    var expectedDataPoints = state.TotalDataPoints;
                    var finalSequenceNumber = state.LastIssuedSequenceNumber;
                    Debug.Log($"RequestUpdateMeasurementSequenceAsync sequenceUuid: {sequenceUuid}, measurementUuid: {measurementUuid}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber}");
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
                        Debug.Log($"RequestUpdateMeasurementSequenceAsync successfully. streamId: {u.Id}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber}");
                    }
                    catch (Exception e)
                    {
                        Debug.Log($"RequestUpdateMeasurementSequenceAsync error. {e.Message}, streamId: {u.Id}, expectedDataPoints: {expectedDataPoints}, finalSequenceNumber: {finalSequenceNumber}");
                        return;
                    }
                }
                {
                    Debug.Log($"RequestEndMeasurementAsync measurementUuid: {measurementUuid}");
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
                        Debug.Log("RequestEndMeasurementAsync successfully.");
                    }
                    catch (Exception e)
                    {
                        Debug.Log($"RequestEndMeasurementAsync error. {e.Message}");
                    }
                }
            }
        });
    }

    public void OnGenerateChunk(Upstream upstream, UpstreamChunk message)
    {
        if (EnableSentDataPointsLog)
            Debug.Log($"[{ConnName}] OnGenerateChunk upstream[{upstream.Id}], SequenceNumber: {message.SequenceNumber}, DataPointCount: {message.DataPointCount}, PayloadSize: {message.PayloadSize} - IUpstreamCallbacks");
        generatedSequenceNumber += 1;
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams)
            {
                if (r.Upstream == upstream)
                {
                    r.Callbacks?.OnGenerateChunk(r, r.SequenceId, message);
                }
            }
        }
    }

    public void OnReceiveAck(Upstream upstream, UpstreamChunkAck message)
    {
        if (EnableSentDataPointsLog)
            Debug.Log($"[{ConnName}] OnReceiveAck upstream[{upstream.Id}], SequenceNumber: {message.SequenceNumber}, ResultCode: {message.ResultCode}, ResultString: {message.ResultString} - IUpstreamCallbacks");
        receivedSequenceNumber += 1;
        lock (upstreamLock)
        {
            foreach (var r in registeredUpstreams)
            {
                if (r.Upstream == upstream)
                {
                    r.Callbacks?.OnReceiveAck(r, r.SequenceId, message);
                }
            }
        }
    }

    public void OnFailWithError(Upstream upstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnFailWithError upstream[{upstream.Id}], error: {error} - IUpstreamCallbacks");
    }

    public void OnCloseWithError(Upstream upstream, Exception error)
    {
        Debug.LogWarning($"[{ConnName}] OnCloseWithError upstream[{upstream.Id}], error: {error} - IUpstreamCallbacks");
        this.Connection?.ReopenUpstream(
            upstream: upstream,
            completion: (newStream, error) =>
            {
                if (newStream == null || error != null)
                {
                    Debug.LogWarning($"[{ConnName}] ReopenUpstream failed. {error?.Message ?? ""}");
                    return;
                }
                lock (upstreamLock)
                {
                    this.usedUpstreams.Add(newStream);
                    newStream.Callbacks = this;
                    IscpUpstream registeredUpstream = null;
                    foreach (var r in registeredUpstreams)
                    {
                        if (r.Upstream == upstream)
                        {
                            registeredUpstream = r;
                            break;
                        }
                    }
                    if (registeredUpstream == null)
                    {
                        Debug.LogWarning($"[{ConnName}] Registerd upstream[{upstream.Id}] not found.");
                        return;
                    }
                    Debug.Log($"[{ConnName}] ReopenUpstream successfully, new upstream[{newStream.Id}]");
                    registeredUpstream.SetUpstream(newStream);
                    registeredUpstream.Callbacks?.OnOpen(registeredUpstream, registeredUpstream.SequenceId);
                    // �����M�̃f�[�^�������Ă���Α��M����
                    if (FailedSendDataPoints[upstream.Id] is List<DataPointGroup> groups)
                    {
                        registeredUpstream.SendDataPoints(groups.ToArray());
                        groups.Clear();
                    }
                    Task.Run(() =>
                    {
                        // �s�v�ɂȂ����X�g���[�������(�������)
                        upstream.Close();
                        this.usedUpstreams.Remove(upstream);
                    });
                }
            });
    }

    public void OnResume(Upstream upstream)
    {
        Debug.Log($"[{ConnName}] OnResume upstream[{upstream.Id}] - IUpstreamCallbacks");
    }
}