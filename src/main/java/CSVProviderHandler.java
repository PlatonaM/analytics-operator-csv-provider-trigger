/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.infai.ses.platonam.util.HttpRequest;
import org.infai.ses.platonam.util.Json;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;

class InitSourceRequest {
    public String source_id;
    public String time_field;
    public String delimiter;
}


class CreateJobRequest {
    public String source_id;
}


public class CSVProviderHandler {
    private static final Logger logger = getLogger(CSVProviderHandler.class.getName());
    private final String csvProviderURL;
    private final String serviceID;
    private final String timeField;
    private final String delimiter;
    private final long delay;
    private String prevJobID = null;

    public CSVProviderHandler(String csvProviderURL, String serviceID, String timeField, String delimiter, long delay) {
        this.csvProviderURL = csvProviderURL;
        this.serviceID = serviceID;
        this.timeField = timeField;
        this.delimiter = delimiter;
        this.delay = delay;
    }

    public void init() throws HttpRequest.HttpRequestException {
        InitSourceRequest request = new InitSourceRequest();
        request.source_id = serviceID;
        request.time_field = timeField;
        request.delimiter = delimiter;
        HttpRequest.httpPost(csvProviderURL + "/data", "application/json", Json.toString(InitSourceRequest.class, request));
        logger.info("initialized for '" + serviceID + "'");
    }

    private String createJob() throws HttpRequest.HttpRequestException {
        CreateJobRequest request = new CreateJobRequest();
        request.source_id = serviceID;
        return HttpRequest.httpPost(csvProviderURL + "/jobs", "application/json", Json.toString(CreateJobRequest.class, request));
    }

    public void startJob() throws HttpRequest.HttpRequestException, InterruptedException {
        String jobID = createJob();
        if (prevJobID == null) {
            prevJobID = jobID;
        } else {
            while (jobID.equals(prevJobID)) {
                logger.info("waiting for previous job '" + prevJobID + "' to complete");
                TimeUnit.SECONDS.sleep(delay);
                jobID = createJob();
            }
        }
        logger.info("started job '" + jobID + "'");
    }
}
