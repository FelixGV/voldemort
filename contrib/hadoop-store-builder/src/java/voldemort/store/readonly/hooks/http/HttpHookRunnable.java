package voldemort.store.readonly.hooks.http;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Basic asynchronous http call.
 */
class HttpHookRunnable implements Runnable {

    private String hookName;
    private Logger log;
    private String urlToCall;
    private HttpMethod httpMethod;
    private String contentType;
    private String requestBody;

    public HttpHookRunnable(String hookName,
                            Logger log,
                            String urlToCall,
                            HttpMethod httpMethod,
                            String contentType,
                            String requestBody) {
        this.hookName = hookName;
        this.log = log;
        this.urlToCall = urlToCall;
        this.httpMethod = httpMethod;
        this.contentType = contentType;
        this.requestBody = requestBody;
    }

    @SuppressWarnings("unchecked")
    public void run() {
        try {
            URL url = new URL(urlToCall);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(httpMethod.name());
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestProperty("Content-Type", contentType);

            if (log.isDebugEnabled())
                log.debug(hookName + " request body: " + requestBody);

            OutputStream out = conn.getOutputStream();
            out.write(requestBody.getBytes());
            out.close();

            if(log.isDebugEnabled()) {
                // Buffer the result into a string
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while((line = rd.readLine()) != null) {
                    sb.append(line);
                }
                rd.close();
                log.debug(hookName + " received response: " + sb);
            }

            if(conn.getResponseCode() != 200) {
                System.out.println(conn.getResponseCode());
                log.error("Illegal response received from " + httpMethod + " request to " + urlToCall);
                throw new IOException(conn.getResponseCode() + ": " + conn.getResponseMessage());
            }

            conn.disconnect();
        } catch(Exception e) {
            log.error("Error while sending a request for a HttpHook [" + hookName + "]", e);
        }
    }
}
