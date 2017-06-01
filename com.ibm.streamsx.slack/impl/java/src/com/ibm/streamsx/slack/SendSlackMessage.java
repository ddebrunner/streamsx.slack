//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.slack;


import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;

import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.samples.patterns.TupleConsumer;

@PrimitiveOperator(
		name="SendSlackMessage", 
		namespace="com.ibm.streamsx.slack",
		description=SendSlackMessage.DESC_OPERATOR
		)
@InputPorts({
	@InputPortSet(
			description="Port that ingests tuples", 
			cardinality=1, 
			optional=false, 
			windowingMode=WindowMode.NonWindowed, 
			windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@Libraries({
	// Include javax.mail libraries.
	"opt/downloaded/*"
	})
public class SendSlackMessage extends TupleConsumer {
	
	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------

	static final String DESC_OPERATOR = 
			"The SendSlackMessage operator extracts the 'message' attribute from "
			+ "incoming tuples and outputs it to the a Slack webhook URL "
			+ "specified in the parameters."
			+ "\\n"
			;
	
	@Parameter(
			optional=false,
			description="Slack incoming webhook URL to send alert to."
			)
	public void setSlackUrl(String slackUrl) throws IOException {
		this.slackUrl = slackUrl;
	}
	
	@Parameter(
			optional=true,
			description="Username to display for alert."
			)
	public void setUsername(String username) throws IOException {
		this.username = username;
	}
	
	@Parameter(
			optional=true,
			description="Icon to display for alert."
			)
	public void setIconUrl(String iconUrl) throws IOException {
		this.iconUrl = iconUrl;
	}
	
	@Parameter(
			optional=true,
			description="Incoming tuple attribute to use as content for message."
			)
	public void setMessageAttribute(String messageAttribute) throws IOException {
		this.messageAttribute = messageAttribute;
	}
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(SendSlackMessage.class.getName());
	
	/**
	 * Slack webhook URL.
	 */
	private String slackUrl;
	
	/**
	 * Username to display.
	 */
	private String username = "SendSlackMessage";
	
	/**
	 * Icon URL.
	 */
	private String iconUrl = "https://www-01.ibm.com/software/data/infosphere/images/InfoSphere-Streams-logo_140x140.png";
	
	/**
	 * Attribute name of tuple to use as message content.
	 */
	private String messageAttribute;
	
	/**
	 * HTTP client and post.
	 */
	HttpClient httpclient;
	HttpPost httppost;
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
	
        httpclient = HttpClients.custom().setConnectionTimeToLive(1000, TimeUnit.MILLISECONDS).setMaxConnPerRoute(1000).build();
        
        // Connect POST to Slack WebHook URL.
		httppost = new HttpPost(slackUrl);
		httppost.addHeader("Content-type", "application/json");
	}

    /**
     * Output message attribute from batched tuple to slack WebHook URL.
     * @param batch
     */
    @Override
    protected boolean processBatch(Queue<BatchedTuple> batch) throws Exception {
    	
    	// Get head tuple in batch.
    	BatchedTuple batchedTuple = batch.peek();
    	Tuple tuple = null;
    	if (batchedTuple != null) {
    		tuple = batchedTuple.getTuple();
    	} else {
    		return true;
    	}
    	
    	// Message to post on slack channel.
    	String message = null;
    	
    	// Try getting "message" attribute from ingested tuple.
    	try {
    		if (messageAttribute == null) {
    			message = tuple.getString("message");
    		} else {
    			message = tuple.getString(messageAttribute);
    		}
    	} catch (Exception e) {
    		_trace.error(e);
    		return true;
    	}
    	
    	// Send Slack message if slack webhook URL is specified.
		if (slackUrl != null) {
			StringEntity params = new StringEntity("{\"text\" : \"" + message + "\""
												+ ", \"username\" : \"" + username + "\"" 
												+ ", \"icon_url\" : \"" + iconUrl + "\"}"
												, "UTF-8");
			params.setContentType("application/json");
			httppost.setEntity(params);
			
			// Attempt to send message.
			HttpResponse response = httpclient.execute(httppost);
			int responseCode = response.getStatusLine().getStatusCode();
			
			// Send successful - remove message from batch queue.
			if (responseCode == 200) {
				batch.remove();
				
				// Can only send 1 message to Slack, per second.
				Thread.sleep(1000);
			}
		}

		return true;
    }

    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }
}


