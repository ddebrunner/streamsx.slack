# streamsx.slack

Toolkit for sending slack messages from a Streams application to a Slack webhook URL.

### Get started developing:

1.  Clone the repository.
2.  In Streams Studio, Import...
3.  In the dialog, select IBM Streams -> SPL Project.
4.  Select com.ibm.streamsx.slack to import the project into Studio.

### Set up an incoming webhook for your Slack channel:

Refer to this guide [here](https://api.slack.com/incoming-webhooks).

### SPLDOC documentation

You can generate HTML from the SPLDOC with the following commands:

```
spl-make-toolkit -i com.ibm.streamsx.slack
spl-make-doc -i com.ibm.streamsx.slack
```

You can view the HTML pages with any browser, or, for example, the following
command:

```
firefox com.ibm.streamsx.slack/doc/spldoc/html/index.html &
```
