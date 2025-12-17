using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using UnityEngine;

/// <summary>
/// HttpClientをシーンを跨いで利用するためのコンポーネント。
/// </summary>
public class IntdashHttpClientPool : MonoBehaviour
{
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
        public readonly string CertId; // Thumbprint 等

        public Key(string baseUrl, string certId)
        {
            BaseUrl = baseUrl ?? "";
            CertId = certId ?? "";
        }

        public bool Equals(Key other) => BaseUrl == other.BaseUrl && CertId == other.CertId;
        public override bool Equals(object obj) => obj is Key k && Equals(k);
        public override int GetHashCode() => HashCode.Combine(BaseUrl, CertId);
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
    /// baseUrl と client cert でキー付けされた HttpClient を取得（同条件なら再利用）。
    /// </summary>
    public static HttpClient Get(string baseUrl, X509Certificate2 cert = null)
    {
        var normalized = NormalizeBase(baseUrl);
        var certId = cert != null ? cert.Thumbprint : "";
        var key = new Key(normalized, certId);

        var lazy = clients.GetOrAdd(key, _ =>
            new Lazy<HttpClient>(() => CreateClient(cert), true));

        return lazy.Value;
    }

    private static HttpClient CreateClient(X509Certificate2 cert)
    {
        var handler = new HttpClientHandler();
        if (handler.SupportsAutomaticDecompression)
            handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

        if (cert != null)
        {
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;
            handler.ClientCertificates.Add(cert);
        }
        return new HttpClient(handler, disposeHandler: true);
    }

    private static void DisposeAll()
    {
        foreach (var kv in clients.ToArray())
            if (kv.Value.IsValueCreated) kv.Value.Value.Dispose();
        clients.Clear();
    }

    private static string NormalizeBase(string url) => url.Trim().TrimEnd('/');
}