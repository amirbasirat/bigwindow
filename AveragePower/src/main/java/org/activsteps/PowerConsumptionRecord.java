package org.activsteps;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * Parses a raw record (line of string data) from data dump into a Java object.
 * 
 * Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;
 * Sub_metering_1;Sub_metering_2;Sub_metering_3
 * 16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000
 * 16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000
 * 16/12/2006;17:26:00;5.374;0.498;233.290;23.000;0.000;2.000;17.000
 * 16/12/2006;17:27:00;5.388;0.502;233.740;23.000;0.000;1.000;17.000
 * 
 */
public class PowerConsumptionRecord {

	Date date;
	Date time;
	double global_Active_Power;

	double global_Reactive_Power;
	double voltage;
	double global_Intensity;

	double Sub_Metering_1;
	double Sub_Metering_2;
	double Sub_Metering_3;
	String[] attributes;
	int year;
	final Calendar cal;
	@SuppressWarnings("deprecation")
	public PowerConsumptionRecord(String inputString)
			throws IllegalArgumentException {
		// header (parsing the inputString is based on this):
		/*
		 * Date; Time; Global_active_power; Global_reactive_power; Voltage;
		 * Global_intensity; Sub_metering_1; Sub_metering_2; Sub_metering_3
		 */

		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat sdt = new SimpleDateFormat("hh:mm:ss");
		
		 cal= Calendar.getInstance();

		attributes = inputString.split(";");

		try {
			/*
			 * Date; Time; Global_active_power; Global_reactive_power; Voltage;
			 * Global_intensity; Sub_metering_1; Sub_metering_2; Sub_metering_3
			 */
			 cal.setTime(sdf.parse(attributes[0]));
			setYear(attributes[0]);
			setDate(sdf.parse(attributes[0]));
			setTime(sdt.parse(attributes[1]));
			setGlobal_Active_Power((Double.parseDouble(attributes[2])));
			setGlobal_Reactive_Power(Double.parseDouble(attributes[3]));
			setVoltage(Double.parseDouble(attributes[4]));
			setGlobal_Intensity(Double.parseDouble(attributes[5]));
			setSub_metering_1((Double.parseDouble(attributes[6])));
			setSub_metering_2(Double.parseDouble(attributes[7]));
			setSub_metering_3(Double.parseDouble(attributes[8]));

		} catch (ParseException e) {
			throw new IllegalArgumentException(
					"Input string contained an unknown value that couldn't be parsed",
					e);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"Input string contained an unknown number value that couldn't be parsed",
					e);
		}

	}

	private void setYear(String date) {
		
		year = cal.get(Calendar.YEAR);
		
	}

	public PowerConsumptionRecord(Text inputText)
			throws IllegalArgumentException {
		this(inputText.toString());
	}

	public static boolean isValidRecord(String rawRecord) {
		String[] tokens = rawRecord.split(";");
		return (tokens.length == 9) && !(rawRecord.contains("?"));
	}

	
	public int getYear() {

		return year;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public double getGlobal_Active_Power() {
		return global_Active_Power;
	}

	public void setGlobal_Active_Power(double global_Active_Power) {
		this.global_Active_Power = global_Active_Power;
	}

	public double getGlobal_Reactive_Power() {
		return global_Reactive_Power;
	}

	public void setGlobal_Reactive_Power(double global_Reactive_Power) {
		this.global_Reactive_Power = global_Reactive_Power;
	}

	public double getVoltage() {
		return voltage;
	}

	public void setVoltage(double voltage) {
		this.voltage = voltage;
	}

	public double getGlobal_Intensity() {
		return global_Intensity;
	}

	public void setGlobal_Intensity(double global_Intensity) {
		this.global_Intensity = global_Intensity;
	}

	public double getSub_metering_1() {
		return Sub_Metering_1;
	}

	public void setSub_metering_1(double sub_metering_1) {
		Sub_Metering_1 = sub_metering_1;
	}

	public double getSub_metering_2() {
		return Sub_Metering_2;
	}

	public void setSub_metering_2(double sub_metering_2) {
		Sub_Metering_2 = sub_metering_2;
	}

	public double getSub_metering_3() {
		return Sub_Metering_3;
	}

	public void setSub_metering_3(double sub_metering_3) {
		Sub_Metering_3 = sub_metering_3;
	}
}
