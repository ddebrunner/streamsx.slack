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

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.model.DefaultAttribute;
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
			"The SendSlackMessage operator outputs the contents of the messageAttribute from "
		  + "incoming tuples to the Slack WebHook URL specified in the parameters."
		  + "\\n"
		  + "The default messageAttribute is: message. This can be changed through the "
		  + "messageAttribute parameter."
		  + "\\n"
		  + "Custom usernames and icons can be used, instead of the default ones, through the "
		  + "usernameAttribute and iconUrlAttribute parameters."
		  + "\\n";
	
	@Parameter(
			optional=false,
			description="Specifies the Slack incoming WebHook URL to send messages to."
			)
	public void setSlackUrl(String slackUrl) throws IOException {
		this.slackUrl = slackUrl;
	}
	
	@Parameter(
			optional=true,
			description="Incoming tuple attribute that specifies the username for the slack message. "
					  + "The default username is specified in the incoming WebHook's configuration."
			)
	public void setUsernameAttribute(TupleAttribute<Tuple, String> usernameAttribute) throws IOException {
		this.usernameAttribute = usernameAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Incoming tuple attribute that specifies the icon URL for the slack message. "
					  + "The default icon is specified in the incoming WebHook's configuration."
			)
	public void setIconUrlAttribute(TupleAttribute<Tuple, String> iconUrlAttribute) throws IOException {
		this.iconUrlAttribute = iconUrlAttribute;
	}
	
	@Parameter(
			optional=true,
			description="Incoming tuple attribute that specifies the icon emoji for the slack message. "
					  + "This will be used in-place of the icon URL, if specified. The incoming WebHook's "
					  + "configuration allows users to choose between an icon or an emoji. If no icon URL or "
					  + "emoji attributes are found, the default icon in the WebHook's configuration will be used."
			)
	public void setIconEmojiAttribute(TupleAttribute<Tuple, String> iconEmojiAttribute) throws IOException {
		this.iconEmojiAttribute = iconEmojiAttribute;
	}
	
	@DefaultAttribute("message")
	@Parameter(
			optional=true,
			description="Incoming tuple attribute to use as content for the slack message. "
					  + "The default attribute to use is 'message'."
			)
	public void setMessageAttribute(TupleAttribute<Tuple, String> messageAttribute) throws IOException {
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
	 * Slack incoming WebHook URL.
	 */
	private String slackUrl;
	
	/**
	 * Attribute containing username to use for message.
	 */
	private TupleAttribute<Tuple, String> usernameAttribute;
	
	/**
	 * Attribute containing icon URL to use for message.
	 */
	private TupleAttribute<Tuple, String> iconUrlAttribute;
	
	/**
	 * Attribute containing icon emoji to use for message.
	 */
	private TupleAttribute<Tuple, String> iconEmojiAttribute;
	
	/**
	 * Attribute containing message to send.
	 */
	private TupleAttribute<Tuple, String> messageAttribute;
	
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
    	
    	// Send Slack message if slack webhook URL is specified.
		if (slackUrl != null) {
			
			// Message to post on slack channel.
	    	String message = messageAttribute.getValue(tuple);
	    	
			JSONObject json = new JSONObject();
			json.put("text", message);
			
			// Override WebHook username and icon/emoji, if params defined.
			if (usernameAttribute != null) {
				json.put("username", usernameAttribute.getValue(tuple));
			}
			if (iconUrlAttribute != null) {
				json.put("icon_url", iconUrlAttribute.getValue(tuple));
			}
			if (iconEmojiAttribute != null) {
				json.put("icon_emoji", iconEmojiAttribute.getValue(tuple));
			}
			
			StringEntity params = new StringEntity(json.toString(), "UTF-8");
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
			} else {
				_trace.error(responseCode + response.toString());
			}
		}

		return true;
    }
    
    public void setBatchSize(int batchSize) { }

    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }
}

