package mt.doris.segmentload.common.http;

import mt.doris.segmentload.common.SegmentLoadException;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import static org.apache.http.client.methods.RequestBuilder.get;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.mortbay.jetty.HttpHeaderValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;

public class HttpUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static CloseableHttpClient createHttpClient(int socketTimeoutMillis) {
        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000) // timeout for grabbing a connection from pool
                .setConnectTimeout(3000) // timeout for tcp connection establishment
                .setSocketTimeout(socketTimeoutMillis) // timeout for request
                .build();
        return HttpClients.custom()
                .disableCookieManagement()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    public static String getTabletMeta(String tabletMetaUrl) throws SegmentLoadException, IOException {
        CloseableHttpClient client = createHttpClient(6000);
        URI tabletMetaUri = UriBuilder.fromUri(tabletMetaUrl).build();
        HttpUriRequest request = get(tabletMetaUri).addHeader(HttpHeaders.EXPECT, HttpHeaderValues.CONTINUE).build();
        try {
            HttpResponse response = client.execute(request);
            String res = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                LOG.error("failed to get tablet meta by url:{}, "
                        + "response code is {}", tabletMetaUrl, response.getStatusLine().getStatusCode());
                throw new SegmentLoadException("fetch tablet meta failed");
            }
            return res;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        } finally {
            client.close();
        }
    }
}
