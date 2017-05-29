//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.slack;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;

import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

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
public class SendSlackMessage extends AbstractOperator {
	
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
	 * Http client, post - connected to Slack webhook URL.
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
        
		httppost = new HttpPost(slackUrl);
		httppost.addHeader("Content-type", "application/json");
	}

    /**
     * Output message attribute to slack webhook URL.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
    	
    	/**
    	 * Message to post on slack channel.
    	 */
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
    		return;
    	}
    	
    	// Send Slack message if slack webhook URL is specified.
		if (slackUrl != null) {
				
			Thread th = new Thread(new SendThread(message));
			th.start();
		
			// Wait for all threads to finish before proceeding.
		    int running = 0;
		    do {
		    	try {
		    	    Thread.sleep(1000);
		    	} catch(InterruptedException ex) {
		    	    Thread.currentThread().interrupt();
		    	}
		    	running = 0;
		    	
		    	if (th.isAlive()) {
		    		running++;
		    	}
		    } while (running > 0);
		}
    }

    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }
    
    public class SendThread extends Thread {
    	
    	String message;
    	
    	public SendThread(String message){
    		this.message = message;
    	}
    	
        public void run(){
    		try {
    			StringEntity params = new StringEntity("{\"text\" : \"" + message + "\""
    												+ ", \"username\" : \"" + username + "\"" 
    												+ ", \"icon_url\" : \"" + iconUrl + "\"}"
    												, "UTF-8");
    			params.setContentType("application/json");
    			httppost.setEntity(params);
    			
				// Try to send message up to 5 times (Slack has limit of 1 message/sec).
				HttpResponse response = null;
				for (int i = 0, responseCode = 0; (i < 5) && (responseCode != 200); i++) {
	    			response = httpclient.execute(httppost);
					responseCode = response.getStatusLine().getStatusCode();
					
					sleep(1000);
				}
				
			} catch (Exception e) {
				_trace.error(e);
			}
        }
    }
    
}
