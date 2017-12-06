package ind.storm.glovanna;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;

import backtype.storm.Config;
import backtype.storm.utils.DRPCClient;

public class GlovannaClient {

	public static void main(String[] args) throws Exception {
		showUserHoursStatitics("1");
		showUserMinutesStatitics("1", 30);

		begAContinuousArea("1", 10);
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
		JSONArray max = null;
		while (it.hasNext()) {
			Object o = it.next();
			JSONArray jo = (JSONArray) o;
			String[] keys = jo.getString(0).split("\\-");
			System.out.println("用户" + keys[0] + "于" + keys[1] + "点内回家" + jo.getLongValue(1) + "次");
			if (max == null) {
				max = jo;
			} else {
				if (max.getLongValue(1) >= jo.getLongValue(1))
					max = jo;
			}
		}
		System.out.println("归家频次最高点钟：" + max.getString(0).split("\\-")[1] + ",建议在59分提醒");
	}

	/**
	 * 
	 * @param userId
	 * @param step
	 *            步长，分钟，将一天按步长划分成区间
	 * @throws Exception
	 */
	public static void showUserMinutesStatitics(String userId, int step) throws Exception {
		int area = 1440 / step;

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

		Map<String, Long> result = new HashMap<String, Long>();
		while (it.hasNext()) {
			Object o = it.next();
			JSONArray jo = (JSONArray) o;
			result.put(jo.getString(0), jo.getLong(1));

			String[] keys = jo.getString(0).split("\\-");

			Long minval = Long.valueOf(keys[1]);
			Long clock = minval / 60;
			Long min = minval - clock * 60;
			System.out.println("用户" + keys[0] + "于" + clock + "点" + min + "分内回家" + jo.getLongValue(1) + "次");

		}

		Object[] max = null;
		for (int i = 0; i < area; i++) {
			long sum = 0;
			for (int j = i * step; j < i * step + step; j++) {
				String tmpKey = userId + "-" + j;
				Long v = result.get(tmpKey);
				if (v != null) {
					sum += v.longValue();
				}
			}
			if (max == null) {
				System.out.println(area + ":" + sum);
				max = new Object[] { i, sum };
			} else {
				if (sum >= (Long) max[1]) {
					max[0] = i;
					max[1] = sum;
				}
			}
		}

		int startHour = (Integer) max[0] * step / 60;
		int startMin = ((Integer) max[0] * step) % 60;
		int endHour = ((Integer) max[0] * step + step) / 60;
		int endMin = ((Integer) max[0] * step + step) % 60;
		System.out.println("The period is from " + startHour + ":" + startMin + " to " + endHour + ":" + endMin
				+ ". The suggestion reminding time is the latter.");
	}

	/**
	 * 获取连续区域，连续的含义为连续的时间区域内有值
	 * 
	 * @param userId
	 * @param step
	 * @throws Exception
	 */
	public static void begAContinuousArea(String userId, int step) throws Exception {

		int area = 1440 / step;

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

		Map<String, Long> result = new HashMap<String, Long>();
		while (it.hasNext()) {
			Object o = it.next();
			JSONArray jo = (JSONArray) o;
			result.put(jo.getString(0), jo.getLong(1));

			String[] keys = jo.getString(0).split("\\-");

			Long minval = Long.valueOf(keys[1]);
			Long clock = minval / 60;
			Long min = minval - clock * 60;
			System.out.println("用户" + keys[0] + "于" + clock + "点" + min + "分内回家" + jo.getLongValue(1) + "次");

		}

		Object[] max = null;
		Map<Integer, Long> areaMap = new HashMap<Integer, Long>();
		for (int i = 0; i < area; i++) {
			long sum = 0;
			for (int j = i * step; j < i * step + step; j++) {
				String tmpKey = userId + "-" + j;
				Long v = result.get(tmpKey);
				if (v != null) {
					sum += v.longValue();
				}
			}
			if (sum > 0)
				areaMap.put(i, sum);
			if (max == null) {
				max = new Object[] { i, sum };
			} else {
				if (sum >= (Long) max[1]) {
					max[0] = i;
					max[1] = sum;
				}
			}
		}

		int startHour = (Integer) max[0] * step / 60;
		int startMin = ((Integer) max[0] * step) % 60;
		int endHour = ((Integer) max[0] * step + step) / 60;
		int endMin = ((Integer) max[0] * step + step) % 60;
		System.out.println("The period is from " + startHour + ":" + startMin + " to " + endHour + ":" + endMin
				+ ". The suggestion reminding time is the latter.");
		System.out.println(max[0]);
		Integer startArea = (Integer) max[0];
		Integer endArea = (Integer) max[0];
		while (true) {
			boolean a = true;
			boolean b = true;
			if (areaMap.get(startArea - 1) != null && areaMap.get(startArea - 1) > 0) {
				a = false;
				startArea--;
			}
			if ((areaMap.get(endArea + 1) != null && areaMap.get(endArea + 1) > 0)) {
				b = false;
				endArea++;
			}
			if (a || b) {
				break;
			}
		}
		System.out.println(startArea);
		startHour = startArea * step / 60;
		startMin = (startArea * step) % 60;
		endHour = (endArea * step + step) / 60;
		endMin = (endArea * step + step) % 60;
		System.out.println("The period is from " + startHour + ":" + startMin + " to " + endHour + ":" + endMin
				+ ". The suggestion reminding time is the latter.");
	}
}
