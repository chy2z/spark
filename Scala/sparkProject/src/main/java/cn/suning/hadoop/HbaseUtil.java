package cn.suning.hadoop;

import com.alibaba.fastjson.JSONObject;
import jodd.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseUtil {

	static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

	private HConnection conn = null;

	public static final String FAMILIY_QUILIFIER = "Familiy:Quilifier";

	public static final String FAMILIY_QUILIFIERVALUE = "Familiy:QuilifierValue";

	private static Map<String, HbaseUtil> HCONNECTION_MAP = new HashMap<String, HbaseUtil>();

	public static final String DEFAULT_HBASE = "default";

	public static final String HBASE_CONFIG_OFFLINE = "hbase-config-offline";

	public static final String HBASE_CONFIG_ONLINE = "hbase-config-online";

	private HbaseUtil() {
		init(null);
	}

	private HbaseUtil(String hbaseConfig) {
		init(hbaseConfig);
	}

	private void init(String hbaseConfig) {
			if (conn == null) {
				try {
					boolean flag=true;
					//System.setProperty("HADOOP_USER_NAME","sousuo");
					Configuration config = HBaseConfiguration.create();
					config.set("hbase.client.operation.timeout","3000");
					config.set("hbase.client.scanner.timeout.period","600000");
					if(hbaseConfig==null||hbaseConfig.equals(DEFAULT_HBASE)) {
						config.addResource(HbaseUtil.class.getClassLoader().getResourceAsStream("hbase-site-dev.xml"));
					}
					else if(hbaseConfig.equals(HBASE_CONFIG_OFFLINE)){
						config.addResource(new Path("hbase-config-offline.xml"));
					}
					else if(hbaseConfig.equals(HBASE_CONFIG_ONLINE)){
						config.addResource(new Path("hbase-config-online.xml"));
					}
					else {
						logger.error("HBase初始化失败 {} 没找到匹配配置", hbaseConfig);
						flag=false;
					}
					if(flag) {
						conn = HConnectionManager.createConnection(config);
					}
				} catch (Exception e) {
					logger.error("HBase初始化失败,", e);
				}
			}
	}

	public static HbaseUtil getClient(String hbaseConfig) {
		HbaseUtil hbaseCllent = HCONNECTION_MAP.get(hbaseConfig);
		if (hbaseCllent == null) {
			synchronized (HbaseUtil.class) {
				hbaseCllent = HCONNECTION_MAP.get(hbaseConfig);
				if (hbaseCllent == null) {
					hbaseCllent = new HbaseUtil(hbaseConfig);
					if (hbaseCllent==null||hbaseCllent.conn == null) {
						return hbaseCllent;
					}
					if (hbaseConfig == null) {
						HCONNECTION_MAP.put(DEFAULT_HBASE, hbaseCllent);
					} else {
						HCONNECTION_MAP.put(hbaseConfig, hbaseCllent);
					}
				}
			}
		}
		return hbaseCllent;
	}

	/**
	 * 根据rwokey查询
	 * @param tableName
	 * @param family
	 * @param column
	 * @param rowKey
	 * @return
	 */
	public Map<String, Object> getResultForMap(String tableName, String family,
			String column, String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		return getResultForMap(tableName, get);
	}

	/**
	 * 根据列查询
	 * @param tableName
	 * @param familyColumn
	 * @param rowKey
	 * @return
	 */
	public Map<String, Object> getResultForMap(String tableName,String[] familyColumn, String rowKey) {

		if (familyColumn != null && familyColumn.length > 0) {
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String fc : familyColumn) {
				String[] fs = fc.split(":");
				if (fs.length == 2) {
					get.addColumn(Bytes.toBytes(fs[0]), Bytes.toBytes(fs[1]));
				}
			}
			return getResultForMapExt(tableName, get);
		}

		return null;
	}

	/**
	 * 根据列查询
	 * @param tableName
	 * @param familyColumn
	 * @param rowKey
	 * @return
	 */
	public Result getResult(String tableName,String[] familyColumn, String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		if (familyColumn != null && familyColumn.length > 0) {
			for (String fc : familyColumn) {
				String[] fs = fc.split(":");
				if (fs.length == 2) {
					get.addColumn(Bytes.toBytes(fs[0]), Bytes.toBytes(fs[1]));
				}
			}
		}
		
		return getResult(tableName, get);
	}

	/**
	 * 根据列簇查询
	 * @param tableName
	 * @param family
	 * @param rowKey
	 * @return
	 */
	public Map<String, Object> getResultForMapByFamily(String tableName,String[] family, String rowKey) {

		if (family != null && family.length > 0) {
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String fc : family) {
				get.addFamily(Bytes.toBytes(fc));
			}
			return getResultForMapExt(tableName, get);
		}

		return null;
	}

	/**
	 * 根据rwokey查询
	 * @param tableName
	 * @param beginKey
	 * @param family
	 * @param column
	 * @param size
	 * @param list
	 * @return
	 */
	public String getAllResultForMap(String tableName, String beginKey,String family, String column, int size,
			List<Map<String, String>> list) {
		Map<String, String> map;
		Scan scan = new Scan();
		ResultScanner rs = null;
		try {
			HTable table = new HTable(conn.getConfiguration(),
					Bytes.toBytes(tableName));
			FilterList filterList = new FilterList(
					FilterList.Operator.MUST_PASS_ALL);
			Filter filter = new PageFilter(size);
			filterList.addFilter(filter);
			scan.setFilter(filterList);
			if (StringUtils.isNotBlank(beginKey)) {
				scan.setStartRow(Bytes.toBytes(beginKey));
			}
			scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
			rs = table.getScanner(scan);
			int i = 0;
			for (Result r : rs) {
				String row = new String(r.getRow());
				if (i == size) {
					return row;
				}
				i++;
				map = new HashMap<String, String>();
				list.add(map);
				for (Cell cell : r.rawCells()) {
					map.put(row, Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
		} catch (Exception e) {
			logger.error("HBase初始化失败,", e);
		} finally {
			if (rs != null) {
				rs.close();
			}
		}

		return null;
	}

	/**
	 * 根据范围查找信息
	 * @param tableName
	 * @param beginKey
	 * @param stopKey
	 * @param family
	 * @param columns
	 * @param maxSize
	 * @param reversed
	 * @return
	 */
	public List<Map<String, String>> getResultForMap(String tableName, String beginKey,String stopKey,
									 String family, String columns,int maxSize,boolean reversed) {
		List<Map<String, String>> list=new ArrayList<>();
		Map<String, String> map;
		Scan scan = new Scan();
		ResultScanner rs = null;
		try {
			HTable table = new HTable(conn.getConfiguration(), Bytes.toBytes(tableName));
			if (StringUtils.isNotBlank(beginKey)) {
				scan.setStartRow(Bytes.toBytes(beginKey));
			}
			if (StringUtils.isNotBlank(stopKey)) {
				scan.setStopRow(Bytes.toBytes(stopKey));
			}
			if(StringUtil.isNotEmpty(columns)) {
				String[] cols = columns.split(",");
				for (String column : cols) {
					scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
				}
			}
			scan.setReversed(reversed);
			rs = table.getScanner(scan);
			int i = 0;
			for (Result r : rs) {
				i++;
				map = new HashMap<String, String>();
				list.add(map);
				for (Cell cell : r.rawCells()) {
					String qualifier = new String(CellUtil.cloneQualifier(cell));
					String value= Bytes.toString(CellUtil.cloneValue(cell));
					map.put(qualifier,value);
				}
				if(i>maxSize){
					break;
				}
			}
		} catch (Exception e) {
			logger.error("HBase初始化失败,", e);
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return list;
	}

	public Map<String, Object> getResultForMap(String tableName, String family,
			String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		return getResultForMap(tableName, get);
	}

	public Map<String, Object> getResultForMap(String tableName, Get get) {
		Map<String, Object> map = new HashMap<String, Object>();
		HTableInterface table = null;
		try {
			table = conn.getTable(tableName);// 获取表
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				logger.info("Rowkey : " + Bytes.toString(result.getRow())
						+ "   Familiy:Quilifier : "
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "   Value : "
						+ Bytes.toString(CellUtil.cloneValue(cell))
						+ "   Time : " + cell.getTimestamp());

				map.put("Rowkey", Bytes.toString(result.getRow()));

				String quilifier = Bytes
						.toString(CellUtil.cloneQualifier(cell));
				String quilifierValue = Bytes.toString(CellUtil
						.cloneValue(cell));
				map.put(FAMILIY_QUILIFIER, quilifier);
				map.put(FAMILIY_QUILIFIERVALUE, quilifierValue);

				map.put("Time", cell.getTimestamp());
			}
		} catch (SocketTimeoutException e) {
			logger.error("读取HBase超时,", e);
		} catch (NotServingRegionException e) {
			logger.error("读取HBase NotServingRegion,", e);
		} catch (Exception e) {
			logger.error("读取HBase失败，", e);
		}

		return map;
	}

	public Map<String, Object> getResultForMapExt(String tableName, Get get) {
		Map<String, Object> map = new HashMap<String, Object>();
		HTableInterface table = null;
		try {
			table = conn.getTable(tableName);// 获取表
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				logger.info("Rowkey : " + Bytes.toString(result.getRow())
						+ "   Familiy:Quilifier : "
						+ Bytes.toString(CellUtil.cloneQualifier(cell))
						+ "   Value : "
						+ Bytes.toString(CellUtil.cloneValue(cell))
						+ "   Time : " + cell.getTimestamp());

				map.put("Rowkey", Bytes.toString(result.getRow()));

				String quilifier = Bytes
						.toString(CellUtil.cloneQualifier(cell));
				String quilifierValue = Bytes.toString(CellUtil
						.cloneValue(cell));
				map.put(quilifier, quilifierValue);

				map.put("Time", cell.getTimestamp());
			}
		} catch (SocketTimeoutException e) {
			logger.error("读取HBase超时,", e);
		} catch (NotServingRegionException e) {
			logger.error("读取HBase NotServingRegion,", e);
		} catch (Exception e) {
			logger.error("读取HBase失败，", e);
		}

		return map;
	}

	public Result getResult(String tableName, Get get) {
		HTableInterface table = null;
		try {
			table = conn.getTable(tableName);// 获取表
			Result result = table.get(get);
			return result;
		} catch (SocketTimeoutException e) {
			logger.error("读取HBase超时,", e);
		} catch (NotServingRegionException e) {
			logger.error("读取HBase NotServingRegion,", e);
		} catch (Exception e) {
			logger.error("读取HBase失败，", e);
		}

		return null;
	}

	/**
	 * 遍历查询hbase表
	 * @param tableName
	 * @throws IOException
	 */
	public void getResultScann(String tableName) throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = null;
		Table table = null;
		try {
			table = conn.getTable(tableName);
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (Cell cell : r.rawCells()) {
					Map<String,String> map=new HashMap<>();
					map.put("Rowkey", Bytes.toString(r.getRow()));
					map.put(",RowName",new String(CellUtil.cloneRow(cell)));
					map.put(",Timetamp",cell.getTimestamp()+"");
					map.put(",ColumnFamily",new String(CellUtil.cloneFamily(cell)));
					map.put(",ColName",new String(CellUtil.cloneQualifier(cell)));
					map.put(",Value",new String(CellUtil.cloneValue(cell)));
					print(JSONObject.toJSONString(map));
				}
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	/**
	 * 根据时间戳范围查询
	 * @param tableName
	 * @param timestampStart
	 * @param timestampEnd
	 * @throws IOException
	 */
	public void getResultScannByTimestampRange(String tableName,String timestampStart,String timestampEnd) throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = null;
		HTableInterface table = null;
		try {
			table = conn.getTable(tableName);// 获取表
			scan.setTimeRange(Long.parseLong(timestampStart),Long.parseLong(timestampEnd));
			rs = table.getScanner(scan);
			for (Result r : rs) {
				print("Rowkey:" + Bytes.toString(r.getRow()));
				for (Cell cell : r.rawCells()) {
					print(",RowName:"+new String(CellUtil.cloneRow(cell))+" ");
					print(",Timetamp:"+cell.getTimestamp()+" ");
					print(",ColumnFamily:"+new String(CellUtil.cloneFamily(cell))+" ");
					print(",ColName:"+new String(CellUtil.cloneQualifier(cell))+" ");
					print(",Value:"+new String(CellUtil.cloneValue(cell))+" ");
					print("########################");
				}
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
	}

	/**
	 * sql 查询 扫描表 指定记录数
	 * @param tableName
	 * @param limit
	 * @return
	 */
	public List<Map<String,Object>> getSqlResultScann(String tableName,int limit) {
		List<Map<String, Object>> ls = new ArrayList<>();
		Map<String, Object> map;
		ResultScanner rs = null;
		HTableInterface table;
		String rowKey = "";
		int top=limit;
		try {
			Scan scan = new Scan();
			table = this.getConn().getTable(tableName);
			rs = table.getScanner(scan);
			if (top > 200) {
				top = 200;
			}
			for (Result r : rs) {
				rowKey = Bytes.toString(r.getRow());
				map = new HashMap<>();
				map.put("rowKey", rowKey);
				for (Cell cell : r.rawCells()) {
					String quilifier = Bytes
							.toString(CellUtil.cloneQualifier(cell));
					String quilifierValue = Bytes.toString(CellUtil
							.cloneValue(cell));
					map.put(quilifier, quilifierValue);
					map.put("Time", cell.getTimestamp());
				}
				ls.add(map);
				if (--top == 0) {
					break;
				}
			}
		} catch (Exception ex) {
			logger.error("XProc#hbase error 2", ex);
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return ls;
	}

	/**
	 * sql 查询 扫描表 指定记录数和列簇
	 * @param tableName
	 * @param family
	 * @param limit
	 * @return
	 */
	public List<Map<String,Object>> getSqlResultScann(String tableName,String[] family,int limit) {
		List<Map<String, Object>> ls = new ArrayList<>();
		Map<String, Object> map;
		ResultScanner rs = null;
		HTableInterface table;
		String rowKey = "";
		int top=limit;
		try {
			table = this.getConn().getTable(tableName);
			Scan scan = new Scan();
			if (family != null && family.length > 0) {
				for (String fc : family) {
					scan.addFamily(Bytes.toBytes(fc));
				}
			}
			rs = table.getScanner(scan);
			if (top > 200) {
				top = 200;
			}
			for (Result r : rs) {
				rowKey = Bytes.toString(r.getRow());
				map = new HashMap<>();
				map.put("rowKey", rowKey);
				for (Cell cell : r.rawCells()) {
					String quilifier = Bytes
							.toString(CellUtil.cloneQualifier(cell));
					String quilifierValue = Bytes.toString(CellUtil
							.cloneValue(cell));
					map.put(quilifier, quilifierValue);
					map.put("Time", cell.getTimestamp());
				}
				ls.add(map);
				if (--top == 0) {
					break;
				}
			}
		} catch (Exception ex) {
			logger.error("XProc#hbase error 2", ex);
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return ls;
	}

	/**
	 * sql 查询 根据rowkey
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public Map<String, Object> getSqlResultForMap(String tableName, String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		return getResultForMapExt(tableName, get);
	}

	/**
	 * sql 查询根据 rowkey 和 列簇
	 * @param tableName
	 * @param family
	 * @param rowKey
	 * @return
	 */
	public Map<String, Object> getSqlResultForMapByFamily(String tableName,
														  String[] family, String rowKey) {

		if (family != null && family.length > 0) {
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String fc : family) {
				get.addFamily(Bytes.toBytes(fc));
			}
			return getResultForMapExt(tableName, get);
		}

		return null;
	}

	/**
	 * 打印分区信息
 	 * @param tableName
	 * @return
	 */
	public void printHTableRegionInfo(String tableName) {
		try {
			HConnection connection = getConn();
			List<HRegionLocation> list = connection.locateRegions(Bytes.toBytes(tableName));
			for (HRegionLocation location : list) {
				HRegionInfo rg = location.getRegionInfo();
				String regionname = Bytes.toString(rg.getRegionName());
				String strkey = Bytes.toString(rg.getStartKey());
				String endkey = Bytes.toString(rg.getEndKey());
				print(regionname);
				print(strkey);
				print(endkey);
			}
		} catch (IOException e) {
			logger.error("printHTableRegionInfo", e);
		}
	}

	public HConnection getConn() {
		return conn;
	}

	public void print(String msg) {
		System.out.println(msg);
	}

	public static void main(String[] args) throws IOException {
		HbaseUtil hbaseCllent = HbaseUtil.getClient(DEFAULT_HBASE);
		hbaseCllent.getResultScann("person");
		hbaseCllent.printHTableRegionInfo("person");

	}

	public static void testQueryOfflineProductByKey() throws IOException{
		String tableName = "ns_sousuo:tf_product_content_info";
		HbaseUtil hbaseCllent = HbaseUtil.getClient(DEFAULT_HBASE);
		hbaseCllent.getResultScann(tableName);
		hbaseCllent.printHTableRegionInfo(tableName);
		List<Map<String, String>> ls =null;
		ls = hbaseCllent.getResultForMap(tableName, "", "50000000", "info", null, 100000000, false);
		ls = hbaseCllent.getResultForMap(tableName, "50000000", "", "info", null, 100000000, false);
    }

	public static void testQueryOfflineProductByTimetamp() throws IOException{
		HbaseUtil hbaseCllent = new HbaseUtil();
		hbaseCllent.getResultScannByTimestampRange("ns_sousuo:tb_search_offline_product","1541649097644",String.valueOf(System.currentTimeMillis()));
    }

	public static void testInsertOfflineProduct(){
		try {
			String table = "ns_sousuo:tf_product_content_info";
			try {
				HbaseUtil hbaseCllent =HbaseUtil.getClient(DEFAULT_HBASE);
				HConnection connection = hbaseCllent.getConn();
				HTableInterface hTable = connection.getTable(table);

				for(int i=1;i<=10;i++) {
					String partnumbers=i+"000000001";
					String key=i+"\\x00ibs##"+partnumbers;
					Put p = new Put(Bytes.toBytes(key));
					p.add(Bytes.toBytes("info"), Bytes.toBytes("partnumbers"), Bytes.toBytes(partnumbers));
					p.add(Bytes.toBytes("info"), Bytes.toBytes("sale"), Bytes.toBytes("电脑"));
					p.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("华为电脑"));
					p.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes("hp"));
					p.add(Bytes.toBytes("info"), Bytes.toBytes("likeCount"), Bytes.toBytes("10"));
					hTable.put(p);
					hbaseCllent.print(key);
				}

				hbaseCllent.print("data Updated");
				hTable.close();
			} catch (Exception e) {
				logger.error("",e);
			}
		} catch (Exception e) {
			logger.error("",e);
		}
	}

	public static void testQueryOfflineProduct(){
		/**
		 * rowkey:partnumber
		 info:code
		 info:sale
		 */
		String table = "ns_sousuo:tb_search_offline_product";
		String family = "info";
		String col = "code";
		String rowkey = "partnumber_2";
		HbaseUtil hbaseCllent = HbaseUtil.getClient("default");
		Map<String, Object> map = hbaseCllent.getResultForMap(table, family, col, rowkey);

	}

	public static void testFindRangeKey(){
		try {
			String table = "ns_sousuo:tb_search_related_keyword";
			String family = "info";
			String col = "column";
			HbaseUtil hbaseCllent = new HbaseUtil();
			HConnection connection = hbaseCllent.getConn();
			HTableInterface hTable = connection.getTable(table);
			String v = "{塑料衣柜=0.02083, 全友衣柜=0.025, 全友=0.02, 拉菲伯爵衣柜=0.04167, 儿童衣柜=0.02865, 尚品宅配=0.02823, 大衣柜=0.01411, 简易衣柜=0.08074, 衣柜 简易 木衣柜=9.3E-4, 全友官方旗舰=0.02, 衣柜=0.33942, 实木衣柜五门衣柜=0.02174, 檀星星衣柜衣帽间=0.02083, 衣橱=0.09992, 木质衣柜=0.0625, 全友家居官方旗舰店=0.01563, 衣柜衣架=0.01563, 衣柜实木=0.17584, 拉菲大衣柜=0.04167, 单人衣柜=0.04167}";
			for (int i = 1; i <= 1; i++) {
                Put p = new Put(Bytes.toBytes("20181102161616_" + String.format("%04d",i)));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("column"), Bytes.toBytes(v));
                hTable.put(p);

				p = new Put(Bytes.toBytes("20181102161716_" +  String.format("%04d",i)));
				p.add(Bytes.toBytes("info"), Bytes.toBytes("column"), Bytes.toBytes(v));
				hTable.put(p);
            }
			hbaseCllent.print("data Updated");
			hTable.close();

			List<Map<String, String>> ls=new ArrayList<>();

			//HBase里面同一列的元素按照rowkey进行排序，排序规则是rowkey的ASCII码排序，小的在前大的在后。

			//扫描结果不包括 stopkey    >=arow_10 and < arow_20
			ls=hbaseCllent.getResultForMap(table,"20181102161616_0001","20181102161616_0010",family,col,100,false);



			System.out.println("====================================");

			//扫描结果不包括 stopkey     <=arow_19 and >arow_09
			ls=hbaseCllent.getResultForMap(table,"20181102161716_0020","20181102161716_0010",family,col,100,true);



		} catch (IOException e) {
			logger.error("",e);
		}
	}

	public static void testInsertKeyWord(){
		try {
			String table = "ns_sousuo:tb_search_related_keyword";
			HbaseUtil hbaseCllent = new HbaseUtil();
			HConnection connection = hbaseCllent.getConn();
			HTableInterface hTable = connection.getTable(table);
			String v = "{塑料衣柜=0.02083, 全友衣柜=0.025, 全友=0.02, 拉菲伯爵衣柜=0.04167, 儿童衣柜=0.02865, 尚品宅配=0.02823, 大衣柜=0.01411, 简易衣柜=0.08074, 衣柜 简易 木衣柜=9.3E-4, 全友官方旗舰=0.02, 衣柜=0.33942, 实木衣柜五门衣柜=0.02174, 檀星星衣柜衣帽间=0.02083, 衣橱=0.09992, 木质衣柜=0.0625, 全友家居官方旗舰店=0.01563, 衣柜衣架=0.01563, 衣柜实木=0.17584, 拉菲大衣柜=0.04167, 单人衣柜=0.04167}";
			for (int i = 1; i <= 1; i++) {
                Put p = new Put(Bytes.toBytes("row_" + i));
                p.add(Bytes.toBytes("info"), Bytes.toBytes("column"), Bytes.toBytes(v));
                hTable.put(p);
            }
			hbaseCllent.print("data Updated");
			hTable.close();
		} catch (Exception e) {
			logger.error("",e);
		}
	}

	public static void testTableRegion() {
		try {
			String table = "ns_sousuo:tb_search_related_keyword";
			HbaseUtil hbaseCllent = new HbaseUtil();
			HConnection connection = hbaseCllent.getConn();
			HTableInterface hTable = connection.getTable(table);
			List<HRegionLocation> list = connection.locateRegions(hTable
					.getName());
			for (HRegionLocation location : list) {
				HRegionInfo rg = location.getRegionInfo();
				String strkey = Bytes.toString(rg.getStartKey());
				String endkey = Bytes.toString(rg.getEndKey());
				System.out.println("Start:" + strkey + ", End:" + endkey);
			}
			hTable.close();
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
