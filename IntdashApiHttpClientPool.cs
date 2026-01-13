using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using UnityEngine;

/// <summary>
/// HttpClientをシーンを跨いで利用するためのコンポーネント。
/// </summary>
public class IntdashHttpClientPool : MonoBehaviour
{
    public const double HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS = 60;

    private static IntdashHttpClientPool instance;

    [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.BeforeSceneLoad)]
    private static void Initialize()
    {
        if (instance == null)
        {
            instance = FindObjectOfType<IntdashHttpClientPool>();
            if (instance == null)
            {
                instance = new GameObject(nameof(IntdashHttpClientPool)).AddComponent<IntdashHttpClientPool>();
                DontDestroyOnLoad(instance.gameObject);
            }
        }

        ServicePointManager.DefaultConnectionLimit = 256;
    }

    private readonly struct Key : IEquatable<Key>
    {
        public readonly string BaseUrl;
        public readonly string CertId; // Thumbprint
        public readonly double TimeoutSeconds; // -1 = infinite

        public Key(string baseUrl, string certId, double timeoutSeconds)
        {
            BaseUrl = baseUrl ?? "";
            CertId = certId ?? "";
            TimeoutSeconds = timeoutSeconds;
        }

        public bool Equals(Key other) =>
            BaseUrl == other.BaseUrl &&
            CertId == other.CertId &&
            TimeoutSeconds == other.TimeoutSeconds;

        public override bool Equals(object obj) => obj is Key k && Equals(k);
        public override int GetHashCode() => HashCode.Combine(BaseUrl, CertId, TimeoutSeconds);
    }

    private bool isApplicationQuitting = false;
    private void OnApplicationQuit()
    {
        Debug.Log($"OnApplicationQuit - IntdashHttpClientPool");
        isApplicationQuitting = true;
    }

    private void OnDestroy()
    {
        Debug.Log("OnDestroy - IntdashHttpClientPool");
        if (!isApplicationQuitting) return;

        DisposeAll();
    }

    private static readonly ConcurrentDictionary<Key, Lazy<HttpClient>> clients = new();

    /// <summary>
    /// baseUrl と client cert とタイムアウト秒数でキー付けされた HttpClient を取得（同条件なら再利用）。
    /// </summary>
    /// <returns>新規作成された場合 true、既存のものが返された場合 false。</returns>
    public static bool Get(string baseUrl, out HttpClient httpClient, double timeoutSeconds = HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS, X509Certificate2 cert = null)
    {
       var normalized = NormalizeBase(baseUrl);

        // 正規化（0以下は infinite 扱い）
        var ts = timeoutSeconds > 0 ? timeoutSeconds : -1;

        var certId = cert != null ? cert.Thumbprint : "";
        var key = new Key(normalized, certId, ts);

        var newLazy = new Lazy<HttpClient>(() => CreateClient(cert, ts), isThreadSafe: true);
        var actualLazy = clients.GetOrAdd(key, newLazy);

        var createdNew = ReferenceEquals(actualLazy, newLazy);

        httpClient = actualLazy.Value;
        return createdNew;
    }

    private static HttpClient CreateClient(X509Certificate2 cert, double timeoutSeconds = HTTP_CLIENT_DEFAULT_TIMEOUT_SECONDS)
    {
        var handler = new HttpClientHandler();
        if (handler.SupportsAutomaticDecompression)
            handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

        if (cert != null)
        {
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;
            handler.ClientCertificates.Add(cert);
        }

        var client = new HttpClient(handler, disposeHandler: true);

        client.Timeout = timeoutSeconds > 0
            ? TimeSpan.FromSeconds(timeoutSeconds)
            : Timeout.InfiniteTimeSpan;

        return client;
    }

    private static void DisposeAll()
    {
        foreach (var kv in clients.ToArray())
            if (kv.Value.IsValueCreated) kv.Value.Value.Dispose();
        clients.Clear();
    }

    private static string NormalizeBase(string url) => url.Trim().TrimEnd('/');
}