package com.shuai.hadoop.homework.job.cstype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IDWriteable implements Writable {

	private Text id;
	private Text time;

	public IDWriteable() {
		this.id = new Text();
		this.time = new Text();
	}

	public Text getId() {
		return id;
	}

	public void setId(Text id) {
		this.id = id;
	}

	public Text getTime() {
		return time;
	}

	public void setTime(Text time) {
		this.time = time;
	}

	public void setALL(String time, String uid) {
		this.id.set(uid);
		this.time.set(time);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		time.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		time.write(out);
	}

}
