package com.findwise.hydra.net;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.CachingDocumentNIO;
import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class MarkHandler<T extends DatabaseType> implements ResponsibleHandler {
    private enum Mark {
        PENDING, PROCESSED, DISCARDED, FAILED
    }

    ;

    private static Logger logger = LoggerFactory.getLogger(MarkHandler.class);

    private CachingDocumentNIO<T> io;
    private boolean performanceLogging = false;

    public MarkHandler(CachingDocumentNIO<T> io, boolean performanceLogging) {
        this.io = io;
        this.performanceLogging = performanceLogging;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response,
                       HttpContext context) throws HttpException, IOException {
        long start = System.currentTimeMillis();
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
                .getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        long tostring = System.currentTimeMillis();
        String stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);
        if (stage == null) {
            HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
            return;
        }

        DatabaseDocument<T> databaseDocument;
        try {
            logger.debug("Received document '{}'", requestContent);
            databaseDocument = io.convert(new LocalDocument(requestContent));
        } catch (JsonException e) {
            HttpResponseWriter.printJsonException(response, e);
            return;
        } catch (ConversionException e) {
            logger.error("Caught Exception when trying to convert " + requestContent, e);
            HttpResponseWriter.printBadRequestContent(response);
            return;
        }
        long convert = System.currentTimeMillis();
        Mark requestedMark = getMark(request);
        logger.debug("Stage '{}' requested Mark '{}' for document with id '{}'", stage, requestedMark, databaseDocument.getID());
        boolean markWasSuccessful = mark(databaseDocument, stage, requestedMark);
        long mark = System.currentTimeMillis();
        if (markWasSuccessful) {
            HttpResponseWriter.printSaveOk(response, databaseDocument.getID());
        } else {
            HttpResponseWriter.printNoDocument(response); // Failure does not always mean the document is missing
        }
        if (performanceLogging) {
            long end = System.currentTimeMillis();
            logger.info(String.format("type=performance event=processed stage_name=%s doc_id=%s start=%d end=%d total=%d entitystring=%d parse=%d mark=%d serialize=%d",
                    stage, databaseDocument.getID(), start, end, end - start, tostring - start, convert - tostring, mark - convert, end - mark));
        }
    }

    private Mark getMark(HttpRequest request) {
        String uri = RESTTools.getBaseUrl(request);
        if (uri.equals(RemotePipeline.PROCESSED_DOCUMENT_URL)) {
            return Mark.PROCESSED;
        } else if (uri.equals(RemotePipeline.PENDING_DOCUMENT_URL)) {
            return Mark.PENDING;
        } else if (uri.equals(RemotePipeline.DISCARDED_DOCUMENT_URL)) {
            return Mark.DISCARDED;
        } else if (uri.equals(RemotePipeline.FAILED_DOCUMENT_URL)) {
            return Mark.FAILED;
        }
        return null;
    }

    private boolean mark(DatabaseDocument<T> databaseDocument, String stage, Mark mark) throws IOException {
        logger.trace("handleMark(..., ..., " + mark.toString() + ")");

        switch (mark) {
            case PENDING: {
                return io.markPending(databaseDocument, stage);
            }
            case PROCESSED: {
                return io.markProcessed(databaseDocument, stage);
            }
            case FAILED: {
                return io.markFailed(databaseDocument, stage);
            }
            case DISCARDED: {
                return io.markDiscarded(databaseDocument, stage);
            }
        }
        return false;
    }

    @Override
    public boolean supports(HttpRequest request) {
        return RESTTools.isPost(request) && getMark(request) != null;
    }

    @Override
    public String[] getSupportedUrls() {
        return new String[]{RemotePipeline.DISCARDED_DOCUMENT_URL,
                RemotePipeline.FAILED_DOCUMENT_URL,
                RemotePipeline.PROCESSED_DOCUMENT_URL,
                RemotePipeline.PENDING_DOCUMENT_URL};
    }

}
