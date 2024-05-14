using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using intdash.Api;
using intdash.Client;
using intdash.Model;
using iSCP.Helpers;
using UnityEngine;

public class IntdashApiManager : MonoBehaviour
{
    public static HttpClientHandler GenerateHttpClientHandler()
    {
        ServicePointManager.DefaultConnectionLimit = 256;
        return new HttpClientHandler();
    }
    public HttpClient HttpClient = new HttpClient(GenerateHttpClientHandler());

    public Configuration Configuration { private set; get; } = new Configuration();

    public static IntdashApiManager Shared { private set; get; }
    [SerializeField] private bool isShared = true;

    [SerializeField] private string _ServerUrl = "https://example.intdash.jp";
    public string ServerUrl
    {
        get => _ServerUrl;
        set
        {
            _ServerUrl = value;
            SetMetadata();
        }
    }
    [SerializeField] private string _ServerPath = "/api";
    public string ServerPath
    {
        get => _ServerPath;
        set
        {
            _ServerPath = value;
            SetMetadata();
        }
    }

    /// <summary>
    /// ServerURL + ServerPath
    /// </summary>
    public string BasePath { get => ServerUrl + ServerPath; }

    public enum AuthorizationType
    {
        [IntdashEnumItem("API Token")]
        ApiToken = 0,
        [IntdashEnumItem("Client Secret (Edge UUID)")]
        EdgeClientSecret = 1,
        [IntdashEnumItem("Client Secret (OAuth2)")]
        OAuth2ClientSecret = 2
    }
    [IntdashLabel("Authorization Type"), IntdashEnum(typeof(AuthorizationType))]
    public AuthorizationType Type = AuthorizationType.ApiToken;

    [Serializable]
    public class APITokenAuthorizationInfo
    {
        /// <summary>
        /// ユーザーのAPIトークン（APIトークンはMy Pageで作成できます）
        /// </summary>
        public string ApiToken = "";
    }
    [SerializeField]
    [IntdashLabel("Authorization Information")]
    [IntdashVisiblity(nameof(Type), AuthorizationType.ApiToken, invisible: true)]
    private APITokenAuthorizationInfo apiTokenInfo = new APITokenAuthorizationInfo();
    public APITokenAuthorizationInfo ApiTokenInfo
    {
        get => apiTokenInfo;
        set
        {
            apiTokenInfo = value;
            SetMetadata();
        }
    }

    [Serializable]
    public class EdgeClientSecretAuthorizationInfo
    {
        [IntdashLabel("Client ID (Edge UUID)")]
        /// <summary>
        /// クライアントID。
        /// </summary>
        public string ClientId = "";

        /// <summary>
        /// エッジの作成時に発行されるクライアントシークレット。
        /// </summary>
        public string ClientSecret = "";

        /// <summary>
        /// アクセストークン。
        /// </summary>
        public string AccessToken { get; set; }

        /// <summary>
        /// リフレッシュトークン。
        /// </summary>
        public string RefreshToken { get; set; } = "";

        /// <summary>
        /// アクセストークンの残り秒数。
        /// </summary>
        public long? ExpiresIn { get; set; }

        /// <summary>
        /// リフレッシュトークンの残り秒数。
        /// </summary>
        public long? RefreshTokenExpiresIn { get; set; }

        /// <summary>
        /// アクセストークンの取得日時。
        /// </summary>
        public long? AcquiredTime { get; set; }

        /// <summary>
        /// リフレッシュトークンが更新可能か。
        /// </summary>
        public bool IsRefreshTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // 残り秒数
                if (!(this.RefreshTokenExpiresIn is long expiresIn))
                {
                    return false;
                }

                // 経過秒数と比較
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }

        /// <summary>
        /// アクセストークンが更新可能かどうか。
        /// </summary>
        public bool IsAccessTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // 残り秒数
                if (!(this.ExpiresIn is long expiresIn))
                {
                    return false;
                }

                // 経過秒数と比較
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }
    }
    [SerializeField]
    [IntdashLabel("Authorization Information")]
    [IntdashVisiblity(nameof(Type), AuthorizationType.EdgeClientSecret, invisible: true)]
    private EdgeClientSecretAuthorizationInfo edgeClientSecretInfo = new EdgeClientSecretAuthorizationInfo();
    public EdgeClientSecretAuthorizationInfo EdgeClientSecretInfo
    {
        get => edgeClientSecretInfo;
        set
        {
            edgeClientSecretInfo = value;
            SetMetadata();
        }
    }

    [Serializable]
    public class OAuth2ClientSecretAuthorizationInfo
    {
        [IntdashLabel("Client ID")]
        /// <summary>
        /// OAuth2認可用のクライアントID。
        /// </summary>
        public string ClientId = "";

        /// <summary>
        /// クライアントID。
        /// </summary>
        public string ClientSecret = "";

        /// <summary>
        /// アクセストークン。
        /// </summary>
        public string AccessToken { get; set; }

        /// <summary>
        /// リフレッシュトークン。
        /// </summary>
        public string RefreshToken { get; set; } = "";

        /// <summary>
        /// アクセストークンの残り秒数。
        /// </summary>
        public long? ExpiresIn { get; set; }

        /// <summary>
        /// リフレッシュトークンの残り秒数。
        /// </summary>
        public long? RefreshTokenExpiresIn { get; set; }

        /// <summary>
        /// アクセストークンの取得日時。
        /// </summary>
        public long? AcquiredTime { get; set; }

        /// <summary>
        /// リフレッシュトークンが更新可能か。
        /// </summary>
        public bool IsRefreshTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // 残り秒数
                if (!(this.RefreshTokenExpiresIn is long expiresIn))
                {
                    return false;
                }

                // 経過秒数と比較
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }

        /// <summary>
        /// アクセストークンが更新可能かどうか。
        /// </summary>
        public bool IsAccessTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // 残り秒数
                if (!(this.ExpiresIn is long expiresIn))
                {
                    return false;
                }

                // 経過秒数と比較
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }
    }
    [SerializeField]
    [IntdashLabel("Authorization Information")]
    [IntdashVisiblity(nameof(Type), AuthorizationType.OAuth2ClientSecret, invisible: true)]
    private OAuth2ClientSecretAuthorizationInfo oAuth2ClientSecretInfo = new OAuth2ClientSecretAuthorizationInfo();
    public OAuth2ClientSecretAuthorizationInfo OAuth2ClientSecretInfo
    {
        get => oAuth2ClientSecretInfo;
        set
        {
            oAuth2ClientSecretInfo = value;
            SetMetadata();
        }
    }

    /// <summary>
    /// アクセストークン。
    /// </summary>
    public string AccessToken
    {
        get
        {
            switch (Type)
            {
                case AuthorizationType.ApiToken:
                    return ApiTokenInfo?.ApiToken;
                case AuthorizationType.EdgeClientSecret:
                    return EdgeClientSecretInfo?.AccessToken;
                case AuthorizationType.OAuth2ClientSecret:
                    return OAuth2ClientSecretInfo?.AccessToken;
            }
            return null;
        }
    }

    [IntdashLabel("Project UUID")]
    /// <summary>
    /// プロジェクトID。
    /// </summary>
    public string ProjectUuid = "";

    /// <summary>
    /// intdashサーバーのAPIバージョン。
    /// 正しく、アクセス情報が設定されていれば Start() 後にセットされます。
    /// </summary>
    public string ApiVersion;

    /// <summary>
    /// APIへのアクセスが有効かどうか。
    /// </summary>
    public bool IsEnableApi => !string.IsNullOrEmpty(AccessToken);

    private Lock refreshTokenLock = new Lock();

    public static IntdashApiManager GetOrCreateSharedInstance()
    {
        if (Shared != null)
            return Shared;
        var instance = FindObjectOfType<IntdashApiManager>();
        if (instance != null)
            return instance;
        var obj = new GameObject(nameof(IntdashApiManager));
        var script = obj.AddComponent<IntdashApiManager>();
        return script;
    }

    private void Awake()
    {
        Debug.Log($"Awake - IntdashApiManager");
        SetMetadata();
    }

    private void OnDestroy()
    {
        Debug.Log($"OnDestroy - IntdashApiManager");
        if (Shared == this)
            Shared = null;
        HttpClient.Dispose();
    }

    public delegate void EnableAPIListener(string version);

    /// <summary>
    /// APIへのアクセスが有効になるとコールされるイベント。
    /// </summary>
    public event EnableAPIListener OnEnableApi;

    private void Start()
    {
        Debug.Log($"Start - IntdashApiManager");
        if (isShared)
            Shared = this;
        SetMetadata();
        Task.Run(async () =>
        {
            // 認証不要
            try
            {
                Debug.Log("InvokeGetVersion()");
                var api = new VersionsVersionApi(HttpClient, Configuration);
                var res = await api.GetVersionAsync().ConfigureAwait(false);
                ApiVersion = res._Version;
                Debug.Log("OnReceiveGetVersion intdash API version: " + ApiVersion);
            }
            catch (Exception e)
            {
                Debug.LogError("Failed to request version. " + e.Message);
                return;
            }
            // アクセストークン取得。
            if (Type == AuthorizationType.EdgeClientSecret
            || Type == AuthorizationType.OAuth2ClientSecret)
            {
                var e = await UpdateAccessTokenWithClientSecretAsync().ConfigureAwait(false);
                if (e != null)
                {
                    Debug.LogError("Failed to get access token. " + e.Message);
                    return;
                }
            }
            switch (Type)
            {
                case AuthorizationType.ApiToken:
                    {
                        try
                        {
                            Debug.Log("InvokeGetMe()");
                            var response = await (new AuthMeApi(HttpClient, Configuration)).GetMeAsync().ConfigureAwait(false);
                            Debug.Log($"ResponseGetMe name: {response.Name}, uuid: {response.Uuid}");
                        }
                        catch (Exception e)
                        {
                            Debug.LogWarning("Failed to GetMe access. " + e.Message);
                        }
                    }
                    break;
                case AuthorizationType.EdgeClientSecret:
                    {
                        try
                        {
                            Debug.Log("InvokeGetMeAsEdge()");
                            var response = await (new AuthEdgesApi(HttpClient, Configuration)).GetMeAsEdgeAsync().ConfigureAwait(false);
                            Debug.Log($"OnReceiveGetMeAsEdge name: {response.Name}, uuid: {response.Uuid}");
                        }
                        catch (Exception e)
                        {
                            Debug.LogWarning("Failed to GetMeAsEdge access. " + e.Message);
                        }
                    }
                    break;
                case AuthorizationType.OAuth2ClientSecret:
                    {
                        try
                        {
                            Debug.Log("InvokeGetMeAsEdge()");
                            var response = await (new AuthEdgesApi(HttpClient, Configuration)).GetMeAsEdgeAsync().ConfigureAwait(false);
                            Debug.Log($"OnReceiveGetMeAsEdge name: {response.Name}, uuid: {response.Uuid}");
                        }
                        catch (Exception e)
                        {
                            Debug.LogWarning("Failed to GetMeAsEdge access. " + e.Message);
                        }
                    }
                    break;
            }
            OnEnableApi?.Invoke(ApiVersion);
        });
    }

    /// <summary>
    /// 有効なトークンを取得します。
    /// <para>トークンの有効期限が切れていればトークンのリフレッシュを行います</para>
    /// </summary>
    public void GetEnableToken(Action<string> completion)
    {
        if (Type == AuthorizationType.ApiToken)
        {
            completion(ApiTokenInfo.ApiToken);
            return;
        }
        this.refreshTokenLock.Wait();
        if (EdgeClientSecretInfo.IsAccessTokenRefreshable)
        {
            this.refreshTokenLock.Release();
            completion?.Invoke(AccessToken);
            return;
        }
        Task.Run(async () =>
        {
            await UpdateAccessTokenWithRefreshTokenAsync().ConfigureAwait(false);
            this.refreshTokenLock.Release();
            if (Type == AuthorizationType.EdgeClientSecret)
            {
                completion?.Invoke(EdgeClientSecretInfo.IsAccessTokenRefreshable ? this.AccessToken : null);
            }
            else if (Type == AuthorizationType.OAuth2ClientSecret)
            {
                completion?.Invoke(OAuth2ClientSecretInfo.IsAccessTokenRefreshable ? this.AccessToken : null);
            }
            else
            {
                completion?.Invoke(null);
            }
        });
    }

    /// <summary>
    /// 有効なトークンを取得します。
    /// <para>トークンの有効期限が切れていればトークンのリフレッシュを行います</para>
    /// </summary>
    public async Task<string> GetEnableTokenAsync()
    {
        if (Type == AuthorizationType.ApiToken)
        {
            return ApiTokenInfo.ApiToken;
        }
        try
        {
            this.refreshTokenLock.Wait();
            if (Type == AuthorizationType.EdgeClientSecret)
            {
                if (EdgeClientSecretInfo.IsAccessTokenRefreshable)
                {
                    return AccessToken;
                }
                if (EdgeClientSecretInfo.IsRefreshTokenRefreshable)
                {
                    await UpdateAccessTokenWithRefreshTokenAsync().ConfigureAwait(false);
                }
                else
                {
                    await UpdateAccessTokenWithClientSecretAsync().ConfigureAwait(false);
                }
                return (EdgeClientSecretInfo.IsAccessTokenRefreshable ? this.AccessToken : null);
            }
            else if (Type == AuthorizationType.OAuth2ClientSecret)
            {
                if (OAuth2ClientSecretInfo.IsAccessTokenRefreshable)
                {
                    return AccessToken;
                }
                if (OAuth2ClientSecretInfo.IsRefreshTokenRefreshable)
                {
                    await UpdateAccessTokenWithRefreshTokenAsync().ConfigureAwait(false);
                }
                else
                {
                    await UpdateAccessTokenWithClientSecretAsync().ConfigureAwait(false);
                }
                return (OAuth2ClientSecretInfo.IsAccessTokenRefreshable ? this.AccessToken : null);
            }
            return null;
        }
        finally
        {
            this.refreshTokenLock.Release();
        }
    }


    public async Task<Exception> UpdateAccessTokenWithClientSecretAsync(System.Threading.CancellationToken cancellationToken = default(System.Threading.CancellationToken))
    {
        try
        {
            var api = new AuthOAuth2Api(HttpClient, Configuration);
            var res = await api.IssueTokenAsync(
                grantType: "client_credentials",
                clientId: EdgeClientSecretInfo.ClientId,
                clientSecret: EdgeClientSecretInfo.ClientSecret,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            ProcessAccessTokenResponse(res);
        }
        catch (Exception e)
        {
            return e;
        }
        return null;
    }

    public async Task<Exception> UpdateAccessTokenWithRefreshTokenAsync()
    {
        try
        {
            var api = new AuthOAuth2Api(HttpClient, Configuration);
            var res = await api.IssueTokenAsync(
                grantType: "refresh_token",
            refreshToken: EdgeClientSecretInfo.RefreshToken,
                clientId: EdgeClientSecretInfo.ClientId).ConfigureAwait(false);
            ProcessAccessTokenResponse(res);
        }
        catch (Exception e)
        {
            return e;
        }
        return null;
    }

    private void ProcessAccessTokenResponse(IssueToken200Response res)
    {
        var now = DateTime.UtcNow;
        if (Type == AuthorizationType.EdgeClientSecret)
        {
            EdgeClientSecretInfo.AccessToken = res.AccessToken;
            EdgeClientSecretInfo.RefreshToken = res.RefreshToken;
            EdgeClientSecretInfo.ExpiresIn = res.ExpiresIn * TimeUtils.TICKS_SEC;
            EdgeClientSecretInfo.RefreshTokenExpiresIn = res.RefreshTokenExpiresIn * TimeUtils.TICKS_SEC;
            EdgeClientSecretInfo.AcquiredTime = now.Ticks;
        }
        else if (Type == AuthorizationType.OAuth2ClientSecret)
        {
            OAuth2ClientSecretInfo.AccessToken = res.AccessToken;
            OAuth2ClientSecretInfo.RefreshToken = res.RefreshToken;
            OAuth2ClientSecretInfo.ExpiresIn = res.ExpiresIn * TimeUtils.TICKS_SEC;
            OAuth2ClientSecretInfo.RefreshTokenExpiresIn = res.RefreshTokenExpiresIn * TimeUtils.TICKS_SEC;
            OAuth2ClientSecretInfo.AcquiredTime = now.Ticks;
        }
        SetMetadata();
    }

    private void SetMetadata()
    {
        Configuration.BasePath = BasePath;
        switch (Type)
        {
            case AuthorizationType.ApiToken:
                if (ApiTokenInfo != null)
                {
                    Configuration.AddApiKey("X-Intdash-Token", ApiTokenInfo.ApiToken);
                }
                break;
            case AuthorizationType.EdgeClientSecret:
                if (EdgeClientSecretInfo != null)
                {
                    Configuration.AccessToken = EdgeClientSecretInfo.AccessToken;
                }
                break;
            case AuthorizationType.OAuth2ClientSecret:
                if (OAuth2ClientSecretInfo != null)
                {
                    Configuration.AccessToken = OAuth2ClientSecretInfo.AccessToken;
                }
                break;
        }
    }
}