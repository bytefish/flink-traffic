package de.bytefish.traffic.backend;

import io.nats.client.impl.SSLContextFactory;
import io.nats.client.impl.SSLContextFactoryProperties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public class InsecureSSLFactory implements SSLContextFactory {

    @Override
    public SSLContext createSSLContext(SSLContextFactoryProperties properties) {
        SSLContext sslContext = null;

        try {
            sslContext = SSLContext.getInstance("TLS");

            sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }}, new SecureRandom());

            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}