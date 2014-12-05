package com.shuai.hadoop.homework.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.alibaba.fastjson.JSONArray;
import com.shuai.hadoop.homework.job.cstype.IDWriteable;
import com.shuai.hadoop.homework.job.cstype.RawLog;

public class PVMR {

	public static class PVMapper extends
			Mapper<Object, Text, Text, IDWriteable> {
		final static Log logger = LogFactory.getLog(PVMapper.class);
		private final static IntWritable one = new IntWritable(1);
		private IDWriteable outValue = new IDWriteable();
		private Text outKey = new Text();
		private String tagPrefix = "abort";

		private String getOutputKey(RawLog log) {
			StringBuilder sb = new StringBuilder();
			String ts1 = log.getTimestamp().replaceFirst("[\\d]{4}$", "0000");
			sb.append(ts1);
			sb.append(":");
			sb.append(log.getSrcIp());
			sb.append(":");
			sb.append(log.getUrl());
			return sb.toString();
		}

		@Override
		protected void setup(
				Mapper<Object, Text, Text, IDWriteable>.Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();
			Pattern pattern = Pattern.compile("req_(\\w*?)_.*");
			Matcher matcher = pattern.matcher(fileName);

			try {
				if (matcher.find()) {
					tagPrefix = matcher.group(1);
				}
			} catch (Exception e) {
				logger.warn(String.format("FileName match error", fileName));
			}

		}

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			RawLog rl = new RawLog(value.toString());
			if (rl.getId() == null || rl.getId().equals("")) {
				return;
			}
			outKey.set(getOutputKey(rl));
			outValue.getTime().set(rl.getTimestamp());
			outValue.getId().set(tagPrefix + ":" + rl.getId());
			context.write(outKey, outValue);
		}

		public static class PVReducer extends
				Reducer<Text, IDWriteable, Text, Text> {

			protected void reduce(Text key, Iterable<IDWriteable> values,
					Context output) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				Iterator<IDWriteable> it = values.iterator();
				Set<String> set = new HashSet<String>();
				String lastID = null;
				boolean isMutiIDRD = false;
				;
				while (it.hasNext()) {
					String id = it.next().getId().toString();
					if (lastID == null) {
						lastID = id.substring(0, id.indexOf(":"));
					} else {
						if (!lastID.equals(id.substring(0, id.indexOf(":")))) {
							isMutiIDRD = true;
						}
					}
					set.add(id);
				}

				if (isMutiIDRD) {
					output.write(key, new Text(JSONArray.toJSONString(set)));
				}
			}
		}

	}

}