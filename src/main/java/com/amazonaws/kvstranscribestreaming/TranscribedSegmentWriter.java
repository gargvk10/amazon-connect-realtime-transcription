package com.amazonaws.kvstranscribestreaming;

import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;


import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.transcribestreaming.model.Result;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;

import java.text.NumberFormat;
import java.time.Instant;
import java.util.List;

/**
 * TranscribedSegmentWriter writes the transcript segments to DynamoDB
 *
 * <p>Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.</p>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
public class TranscribedSegmentWriter {

    private String contactId;
    private DynamoDB ddbClient;
    private AmazonTranslate translateClient;
    private AmazonSQS sqsClient;
    private String sqsUrl;
    private Boolean consoleLogTranscriptFlag;
    private static final boolean SAVE_PARTIAL_TRANSCRIPTS = Boolean.parseBoolean(System.getenv("SAVE_PARTIAL_TRANSCRIPTS"));
    private static final Logger logger = LoggerFactory.getLogger(TranscribedSegmentWriter.class);


    /**
     * Now takes in SQS queue URL to distinguish which queue the transcribed segments will be written to.
     */
    public TranscribedSegmentWriter(String contactId, DynamoDB ddbClient, String sqsUrl, Boolean consoleLogTranscriptFlag) {

        this.contactId = Validate.notNull(contactId);
        this.ddbClient = Validate.notNull(ddbClient);
        this.consoleLogTranscriptFlag = Validate.notNull(consoleLogTranscriptFlag);
        this.sqsUrl=sqsUrl;
        translateClient = AmazonTranslateClient.builder().build();
        sqsClient = AmazonSQSClientBuilder.defaultClient();

    }

    public String getContactId() {

        return this.contactId;
    }

    public DynamoDB getDdbClient() {

        return this.ddbClient;
    }

    public AmazonSQS getSqsClient(){
        return this.sqsClient;
    }

    public void writeToDynamoDBAndSQS(TranscriptEvent transcriptEvent, String tableName) {
        logger.info("table name: " + tableName);
        logger.info("Transcription event: " + transcriptEvent.transcript().toString());
        List<Result> results = transcriptEvent.transcript().results();
        if (results.size() > 0) {
            Result result = results.get(0);
            if (SAVE_PARTIAL_TRANSCRIPTS || !result.isPartial()) {
                try {
                    Item ddbItem = toDynamoDbItem(result);
                    if (ddbItem != null) {
                        if(!ddbItem.getBoolean("IsPartial")){
                            String finalTranscript = ddbItem.getString("Transcript");
                            logger.info("Final Untranslated Transcript: "+ finalTranscript);

                            String translatedTranscript = translateText("en", "es", finalTranscript);
                            sendToSQS(translatedTranscript);
                        }

                        logger.info("Putting item in DynamoDB");
                        long cur = System.currentTimeMillis();
                        getDdbClient().getTable(tableName).putItem(ddbItem);
                        long diff = System.currentTimeMillis()-cur;
                        logger.info("Item in DynamoDB (milli): " + diff);
                    }

                } catch (Exception e) {
                    logger.error("Exception while writing to DDB: ", e);
                }
            }
        }
    }

    /**
     * This method uses Amazon Translate Client to translate the text passed as a paramater according to its
     * source language code and target language code
     * @param sourceLangCode This signifies the language the text is written in
     * @param targetLangCode  This signifies the language for the text to translate to
     * @param text This is the text to translate
     * @return String This is the translated text
     */
    private String translateText(String sourceLangCode, String targetLangCode, String text){
        long cur= System.currentTimeMillis();
        logger.info("Starting Translation: " + text);
        try{
            TranslateTextRequest request = new TranslateTextRequest()
                    .withText(text)
                    .withSourceLanguageCode(sourceLangCode)
                    .withTargetLanguageCode(targetLangCode);
            TranslateTextResult translateResult = translateClient.translateText(request);
            long diff = System.currentTimeMillis()-cur;
            String translated = translateResult.getTranslatedText();
            logger.info("Translation: "+ translated);
            logger.info("Translation time (milli): " + diff);
            return translated;
        } catch (Exception e){
            logger.error("Exception while translating transcript: ", e);
            logger.info("Unable to translate. Returning untranslated text");
            return text;
        }
    }

    /**
     * This method uses Amazon SQS client to send the translated message to its respective SQS queue
     * @param message This is the message to send to the SQS Queue
     */
    private void sendToSQS(String message){
        logger.info("Sending Message to SQS");
        long cur = System.currentTimeMillis();
        try{
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(sqsUrl)
                    .withMessageBody(message)
                    .withMessageGroupId("test")
                    .withMessageDeduplicationId("test"+cur);
            getSqsClient().sendMessage(send_msg_request);
            long diff = System.currentTimeMillis()-cur;
            System.out.println("Sending message to SQS time (milli): " + diff);
        } catch (Exception e){
            logger.error("Exception while sending message to SQS: ", e);
        }

    }

    private Item toDynamoDbItem(Result result) {
        logger.info("Creating DynamoDB Item");
        long cur = System.currentTimeMillis();
        String contactId = this.getContactId();
        Item ddbItem = null;
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumFractionDigits(3);
        nf.setMaximumFractionDigits(3);
        if (result.alternatives().size() > 0) {
            if (!result.alternatives().get(0).transcript().isEmpty()) {
                Instant now = Instant.now();
                ddbItem = new Item()
                        .withKeyComponent("ContactId", contactId)
                        .withKeyComponent("StartTime", result.startTime())
                        .withString("SegmentId", result.resultId())
                        .withDouble("EndTime", result.endTime())
                        .withString("Transcript", result.alternatives().get(0).transcript())
                        .withBoolean("IsPartial", result.isPartial())
                        // LoggedOn is an ISO-8601 string representation of when the entry was created
                        .withString("LoggedOn", now.toString())
                        // expire after a week by default
                        .withDouble("ExpiresAfter", now.plusMillis(7 * 24 * 3600).toEpochMilli());

                if (consoleLogTranscriptFlag) {
                    logger.info(String.format("Thread %s %d: [%s, %s] - %s",
                            Thread.currentThread().getName(),
                            System.currentTimeMillis(),
                            nf.format(result.startTime()),
                            nf.format(result.endTime()),
                            result.alternatives().get(0).transcript()));
                }
            }
        }
        long diff = System.currentTimeMillis()-cur;
        logger.info("DynamoDB Item Created (milli): " + diff);
        return ddbItem;
    }
}
