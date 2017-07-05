//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.slack;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
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
import com.ibm.streams.operator.ProcessingElement;
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

	protected static final String DESC_OPERATOR = 
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
			optional=true,
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
	
	@Parameter(
			optional=true,
			description="Name of application configuration containing operator parameters. "
					  + "Parameters of type Attribute should be specified in the form of a String."
			)
	public void setApplicationConfigurationName(String applicationConfigurationName) throws IOException {
		if (!applicationConfigurationName.isEmpty()) {
			this.applicationConfigurationName = applicationConfigurationName;
		}
	}
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(SendSlackMessage.class.getName());
	
	/**
	 * Application configuration key names.
	 */
	private static final String PARAM_SLACK_URL = "slackUrl",
								PARAM_MESSAGE_ATTR = "messageAttribute",
								PARAM_USERNAME_ATTR = "usernameAttribute",
								PARAM_ICON_URL_ATTR = "iconUrlAttribute",
								PARAM_ICON_EMOJI_ATTR = "iconEmojiAttribute";
	
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
	 * Name of application configuration containing operator parameter values.
	 */
	private String applicationConfigurationName;
	
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
    	
    	// Update slackUrl with the one defined in the application configuration.
    	updateSlackUrl();
			
		// Message to post on slack channel.
    	String message = getMessage(tuple);
    	
		JSONObject json = new JSONObject();
		json.put("text", message);
		
		// Override WebHook username and icon/emoji, if params defined.
		String username = getUsername(tuple);
		if (username != null) {
			json.put("username", username);
		}
		
		String iconUrl = getIconUrl(tuple);
		if (iconUrl != null) {
			json.put("icon_url", iconUrl);
		}
		
		String iconEmoji = getIconEmoji(tuple);
		if (iconEmoji != null) {
			json.put("icon_emoji", iconEmoji);
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
    
    /**
     * Update slackUrl with the one defined in the application configuration.
     * @throws Exception
     */
    private void updateSlackUrl() throws Exception {
    	if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAM_SLACK_URL)) {
				String applicationConfigurationSlackUrl = properties.get(PARAM_SLACK_URL);
				if (!applicationConfigurationSlackUrl.equals(slackUrl)) {
					slackUrl = properties.get(PARAM_SLACK_URL);
					httppost = new HttpPost(slackUrl);
					httppost.addHeader("Content-type", "application/json");
				}
				return;
			}
		}
    	
    	// Slack URL in application configuration not defined. Use slackUrl from operator parameters, instead.
    	if (slackUrl != null) {
			httppost = new HttpPost(slackUrl);
			httppost.addHeader("Content-type", "application/json");
		} else {
			throw new Exception(PARAM_SLACK_URL + " can't be found in application configuration or in the operator's parameters.");
		}
    }
    
    /**
  	 * Retrieve message from incoming tuple. If applicationConfigurationName is specified, use 
  	 * message attribute specified in application configuration to get message, instead.
  	 */
  	private String getMessage(Tuple tuple) {
  		if (applicationConfigurationName != null) {
  			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
  			if (properties.containsKey(PARAM_MESSAGE_ATTR)) {
  				String applicationConfigurationMessage = properties.get(PARAM_MESSAGE_ATTR);
  				return tuple.getString(applicationConfigurationMessage);
  			}
  		}
		return messageAttribute.getValue(tuple);
  	}
    
    /**
	 * Retrieve username from incoming tuple. If applicationConfigurationName is specified, use 
	 * username attribute specified in application configuration to get username, instead.
	 */
	private String getUsername(Tuple tuple) {
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAM_USERNAME_ATTR)) {
				String applicationConfigurationUsername = properties.get(PARAM_USERNAME_ATTR);
				return tuple.getString(applicationConfigurationUsername);
			}
			
		} else if (usernameAttribute != null) {
			return usernameAttribute.getValue(tuple);
		}
		return null;
	}
	
    /**
	 * Retrieve iconUrl from incoming tuple. If applicationConfigurationName is specified, use 
	 * iconUrl attribute specified in application configuration to get iconUrl, instead.
	 */
	private String getIconUrl(Tuple tuple) {
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAM_ICON_URL_ATTR)) {
				String applicationConfigurationIconUrl = properties.get(PARAM_ICON_URL_ATTR);
				return tuple.getString(applicationConfigurationIconUrl);
			}
		} else if (iconUrlAttribute != null) {
			return iconUrlAttribute.getValue(tuple);
		}
		return null;
	}
	
    /**
	 * Retrieve iconEmoji from incoming tuple. If applicationConfigurationName is specified, use 
	 * iconEmoji attribute specified in application configuration to get iconEmoji, instead.
	 */
	private String getIconEmoji(Tuple tuple) {
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAM_ICON_EMOJI_ATTR)) {
				String applicationConfigurationIconEmoji = properties.get(PARAM_ICON_EMOJI_ATTR);
				return tuple.getString(applicationConfigurationIconEmoji);
			}
		} else if (iconEmojiAttribute != null) {
			return iconEmojiAttribute.getValue(tuple);
		}
		return null;
	}
    
	/**
	 * Calls the ProcessingElement.getApplicationConfiguration() method to
	 * retrieve the application configuration if application configuration
	 * is supported.
	 * 
	 * @return
	 * The application configuration.
	 */
	@SuppressWarnings("unchecked")
	protected Map<String,String> getApplicationConfiguration(String applicationConfigurationName) {
		Map<String,String> properties = null;
		try {
			ProcessingElement pe = getOperatorContext().getPE();
			Method method = ProcessingElement.class.getMethod("getApplicationConfiguration", new Class[]{String.class});
			Object returnedObject = method.invoke(pe, applicationConfigurationName);
			properties = (Map<String,String>)returnedObject;
		}
		catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			properties = new HashMap<>();
		}
		return properties;
	}
}

