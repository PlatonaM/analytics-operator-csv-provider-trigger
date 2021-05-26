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


import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class Trigger extends BaseOperator {

    private static final Logger logger = getLogger(Trigger.class.getName());
    private final CSVProviderHandler csvProviderHandler;
    private final long messageThreshold;
    private final long delay;
    private long messageCount = 0;

    public Trigger(CSVProviderHandler csvProviderHandler, long messageThreshold, long delay) {
        this.messageThreshold = messageThreshold;
        this.csvProviderHandler = csvProviderHandler;
        this.delay = delay;
    }

    @Override
    public void run(Message message) {
        try {
            if (messageCount == messageThreshold) {
                String timestamp = message.getInput("time").getString();
                logger.info("consumed '" + messageCount + "' messages with last timestamp @ '" + timestamp + "'");
                logger.info("scheduling new job in '" + delay + "' seconds ...");
                TimeUnit.SECONDS.sleep(delay);
                csvProviderHandler.startJob();
                messageCount = 0;
            } else {
                messageCount++;
            }
        } catch (Throwable t) {
            logger.severe("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput("time");
        return message;
    }

}
