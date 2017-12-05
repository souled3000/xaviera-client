package ind.storm.glovanna;

import java.util.Iterator;

import com.alibaba.fastjson.JSONArray;

import backtype.storm.Config;
import backtype.storm.utils.DRPCClient;

public class GlovannaClient {

	public static void main(String[] args) throws Exception {
		showUserHoursStatitics("1");
		showUserMinutesStatitics("1");
	}

	public static void showUserHoursStatitics(String userId) throws Exception {
		Config conf = new Config();
		conf.setDebug(true);
		DRPCClient client = new DRPCClient("192.168.2.177", 3772);

		StringBuilder drpcKey = new StringBuilder();
		for (int i = 0; i < 24; i++)
			drpcKey.append(userId).append("-").append(i).append(":00 ");

		String drpcValue = client.execute("Glovanna", drpcKey.toString());

		JSONArray ary = JSONArray.parseArray(drpcValue);
		Iterator<Object> it = ary.iterator();

		while (it.hasNext()) {
			Object o = it.next();
			JSONArray jo = (JSONArray) o;
			String[] keys = jo.getString(0).split("\\-");
			System.out.println("用户" + keys[0] + "于" + keys[1] + "点内回家" + jo.getLongValue(1) + "次");
		}
	}

	public static void showUserMinutesStatitics(String userId) throws Exception {
		Config conf = new Config();
		conf.setDebug(true);
		DRPCClient client = new DRPCClient("192.168.2.177", 3772);

		StringBuilder drpcKey = new StringBuilder();
		for (int i = 0; i < 1440; i++)
			drpcKey.append(userId).append("-").append(i).append(" ");

		String drpcValue = client.execute("Glovanna", drpcKey.toString());
		System.out.println(drpcValue);
		JSONArray ary = JSONArray.parseArray(drpcValue);
		Iterator<Object> it = ary.iterator();

		while (it.hasNext()) {
			Object o = it.next();
			JSONArray jo = (JSONArray) o;
			String[] keys = jo.getString(0).split("\\-");

			Long minval = Long.valueOf(keys[1]);
			Long clock = minval / 60;
			Long min = minval - clock * 60;
			System.out.println("用户" + keys[0] + "于" + clock + "点" + min + "分内回家" + jo.getLongValue(1) + "次");
		}
	}

}
