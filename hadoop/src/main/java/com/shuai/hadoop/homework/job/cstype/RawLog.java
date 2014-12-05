package com.shuai.hadoop.homework.job.cstype;

public class RawLog {
	private String timestamp = "";
	private String srcIp = "";
	private String id = "";
	private String ua = "";
	private String url = "";

	public RawLog(String lineByTab) {
		String[] a = lineByTab.split("\\t");
		if (a.length == 5) {
			this.timestamp = a[0];
			this.srcIp = a[1];
			this.id = a[2];
			this.ua = a[3];
			this.url = a[4];
		}
	}

	public RawLog(String timestamp, String srcIp, String id, String ua,
			String url) {
		this.timestamp = timestamp;
		this.srcIp = srcIp;
		this.id = id;
		this.ua = ua;
		this.url = url;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getSrcIp() {
		return srcIp;
	}

	public void setSrcIp(String srcIp) {
		this.srcIp = srcIp;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUa() {
		return ua;
	}

	public void setUa(String ua) {
		this.ua = ua;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
