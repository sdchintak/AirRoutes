package com.airport.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
* This is the writable class which will be the value in the mapper. This class will also be used to sort the origin-destination airports by average delay
**/
public class RouteDelay implements WritableComparable {
	Text route;
	DoubleWritable delay;
	
	public RouteDelay() {
		route = new Text();
		delay = new DoubleWritable();
	}
	
	public RouteDelay(String r, Double d) {
		route = new Text(r);
		delay = new DoubleWritable(d);
	}
	
	public RouteDelay(Text r, DoubleWritable d) {
		route = new Text(r);	
		delay = new DoubleWritable(d.get());
	}
	
	/**
	 * @return the route
	 */
	public Text getRoute() {
		return this.route;
	}
	/**
	 * @return the delay
	 */
	public DoubleWritable getDelay() {
		return this.delay;
	}
	/**
	 * @param route the route to set
	 */
	public void setRoute(Text route) {
		this.route = route;
	}
	/**
	 * @param delay the delay to set
	 */
	public void setDelay(DoubleWritable delay) {
		this.delay = delay;
	}
	
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		RouteDelay rd = (RouteDelay) o;
		return this.getDelay().compareTo(rd.getDelay());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		route.write(out);
		delay.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		route.readFields(in);
		delay.readFields(in);
	}
	
	public String toString() {
		return this.route.toString() + "~" + this.delay.toString();
	}

	/* (non-Javadoc)
	 * check if any two RouteDelay objects are the same
	 */
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		RouteDelay rd = (RouteDelay) obj;
		return this.route.equals(rd.getRoute()) && this.delay.equals(rd.getDelay());
	}
}
