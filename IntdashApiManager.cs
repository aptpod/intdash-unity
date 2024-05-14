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
        /// ���[�U�[��API�g�[�N���iAPI�g�[�N����My Page�ō쐬�ł��܂��j
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
        /// �N���C�A���gID�B
        /// </summary>
        public string ClientId = "";

        /// <summary>
        /// �G�b�W�̍쐬���ɔ��s�����N���C�A���g�V�[�N���b�g�B
        /// </summary>
        public string ClientSecret = "";

        /// <summary>
        /// �A�N�Z�X�g�[�N���B
        /// </summary>
        public string AccessToken { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N���B
        /// </summary>
        public string RefreshToken { get; set; } = "";

        /// <summary>
        /// �A�N�Z�X�g�[�N���̎c��b���B
        /// </summary>
        public long? ExpiresIn { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N���̎c��b���B
        /// </summary>
        public long? RefreshTokenExpiresIn { get; set; }

        /// <summary>
        /// �A�N�Z�X�g�[�N���̎擾�����B
        /// </summary>
        public long? AcquiredTime { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N�����X�V�\���B
        /// </summary>
        public bool IsRefreshTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // �c��b��
                if (!(this.RefreshTokenExpiresIn is long expiresIn))
                {
                    return false;
                }

                // �o�ߕb���Ɣ�r
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }

        /// <summary>
        /// �A�N�Z�X�g�[�N�����X�V�\���ǂ����B
        /// </summary>
        public bool IsAccessTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // �c��b��
                if (!(this.ExpiresIn is long expiresIn))
                {
                    return false;
                }

                // �o�ߕb���Ɣ�r
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
        /// OAuth2�F�p�̃N���C�A���gID�B
        /// </summary>
        public string ClientId = "";

        /// <summary>
        /// �N���C�A���gID�B
        /// </summary>
        public string ClientSecret = "";

        /// <summary>
        /// �A�N�Z�X�g�[�N���B
        /// </summary>
        public string AccessToken { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N���B
        /// </summary>
        public string RefreshToken { get; set; } = "";

        /// <summary>
        /// �A�N�Z�X�g�[�N���̎c��b���B
        /// </summary>
        public long? ExpiresIn { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N���̎c��b���B
        /// </summary>
        public long? RefreshTokenExpiresIn { get; set; }

        /// <summary>
        /// �A�N�Z�X�g�[�N���̎擾�����B
        /// </summary>
        public long? AcquiredTime { get; set; }

        /// <summary>
        /// ���t���b�V���g�[�N�����X�V�\���B
        /// </summary>
        public bool IsRefreshTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // �c��b��
                if (!(this.RefreshTokenExpiresIn is long expiresIn))
                {
                    return false;
                }

                // �o�ߕb���Ɣ�r
                var now = DateTime.UtcNow.Ticks;
                var elapsedTime = now - acquiredTime;
                return elapsedTime < expiresIn;
            }
        }

        /// <summary>
        /// �A�N�Z�X�g�[�N�����X�V�\���ǂ����B
        /// </summary>
        public bool IsAccessTokenRefreshable
        {
            get
            {
                if (!(this.AcquiredTime is long acquiredTime))
                {
                    return false;
                }

                // �c��b��
                if (!(this.ExpiresIn is long expiresIn))
                {
                    return false;
                }

                // �o�ߕb���Ɣ�r
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
    /// �A�N�Z�X�g�[�N���B
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
    /// �v���W�F�N�gID�B
    /// </summary>
    public string ProjectUuid = "";

    /// <summary>
    /// intdash�T�[�o�[��API�o�[�W�����B
    /// �������A�A�N�Z�X��񂪐ݒ肳��Ă���� Start() ��ɃZ�b�g����܂��B
    /// </summary>
    public string ApiVersion;

    /// <summary>
    /// API�ւ̃A�N�Z�X���L�����ǂ����B
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
    /// API�ւ̃A�N�Z�X���L���ɂȂ�ƃR�[�������C�x���g�B
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
            // �F�ؕs�v
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
            // �A�N�Z�X�g�[�N���擾�B
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
    /// �L���ȃg�[�N�����擾���܂��B
    /// <para>�g�[�N���̗L���������؂�Ă���΃g�[�N���̃��t���b�V�����s���܂�</para>
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
    /// �L���ȃg�[�N�����擾���܂��B
    /// <para>�g�[�N���̗L���������؂�Ă���΃g�[�N���̃��t���b�V�����s���܂�</para>
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