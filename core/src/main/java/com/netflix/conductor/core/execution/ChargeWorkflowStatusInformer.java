package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class ChargeWorkflowStatusInformer implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChargeWorkflowStatusInformer.class);
    private final Configuration configuration;

    @Inject
    public ChargeWorkflowStatusInformer(final Configuration configuration) {
        this.configuration = configuration;
    }


    String trigger(final String runId,
                   final String status) throws Exception {
        final HttpURLConnection con;

        String url = String.format(configuration.getWorkflowCompletionListenerUrl(), runId, status);
        URL myUrl = new URL(url);
        con = (HttpURLConnection) myUrl.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("User-Agent", "Java client");
        con.setRequestProperty("Content-Type", "application/json");

        StringBuilder content;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream()))) {
            String line;
            content = new StringBuilder();

            while ((line = br.readLine()) != null) {
                content.append(line);
                content.append(System.lineSeparator());
            }
        }
        con.disconnect();
        return content.toString();
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        try {
            trigger(workflow.getWorkflowId(), "Completed");
        } catch (final Exception e) {
            LOGGER.error("Error while logging completed status ", e);
        }
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        try {
            trigger(workflow.getWorkflowId(), "Failed");
        } catch (final Exception e) {
            LOGGER.error("Error while logging terminated status ", e);
        }
    }
}
