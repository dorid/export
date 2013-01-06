import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * User: dori-desktop
 * Date: 12-10-14
 * Time: 下午8:23
 */
public class Export {

    public static void main(String[] args) throws IOException, SQLException {
        File file = new File("C:/name.csv");

        FileOutputStream out = new FileOutputStream(file);
        OutputStreamWriter osw = new OutputStreamWriter(out, "gb2312");
        BufferedWriter bw = new BufferedWriter(osw);
//insert data

        bw.write(getData());

        //close
        bw.close();
        osw.close();
        out.close();
    }

    private static String getData() throws SQLException {

        Map<String, String> multiSonger = getMultiSonger();
        Connection connection = JdbcUtils.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement("select * from analyze_data order by CONVERT( sname USING GBK ) ASC");

        Map map1 = new LinkedHashMap();
        Map map2 = new LinkedHashMap();
        Map map3 = new LinkedHashMap();
        Map all = new LinkedHashMap();

        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String sname = resultSet.getString("sname");
            String province = resultSet.getString("province");
            String download = resultSet.getString("download");
            String songer = resultSet.getString("songer");
            String stype = resultSet.getString("stype");

            //歌名，歌手重复
            if (multiSonger.get(sname) != null) {
                sname = sname + "(" + songer + ")";
            }

            Map tmp = null;
            if (stype.equals("彩铃")) {
                tmp = map1;
            }else if (stype.equals("振铃")) {
                tmp = map2;
            }else if (stype.equals("全曲")) {
                tmp = map3;
            }


            Object o = tmp.get(sname);
            if (o != null) {
                if (((Map) tmp.get(sname)).get(province) != null) {
                    System.out.println("重复");
                }
                ((Map) tmp.get(sname)).put(province, download);
            } else {
                Map subMap = new HashMap();
                subMap.put(province, download);
                tmp.put(sname , subMap);
            }

            all.put(sname, sname);
        }

        JdbcUtils.free(resultSet, preparedStatement, connection);
        return generate(map1, map2, map3, all);
    }


    /*
    * 歌名相同，但作者不同
    * */
    private static Map<String, String> getMultiSonger() throws SQLException {

        Map<String, String> multi = new HashMap<String, String>();

        String sql = "select t.* from (select count(1) count, a.* from (select * from analyze_data group by sname, songer) a group by sname) t where count>1";
        Connection connection = JdbcUtils.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            String sname = resultSet.getString("sname");
            multi.put(sname, sname);
        }

        JdbcUtils.free(resultSet, preparedStatement, connection);
        return multi;  //To change body of created methods use File | Settings | File Templates.
    }

    /*
    * map
    *
    * key:song name
    * value:map
    *   key:province
    *   value:download
    * */
    public static String generate(Map map1, Map map2, Map map3, Map all) throws SQLException {
        
        generateProvince();

        List provinces = new ArrayList();
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select * from province order by province");
        ResultSet resultSet = preparedStatement.executeQuery();
        StringBuffer sb = new StringBuffer("");
        StringBuffer header1 = new StringBuffer(",");
        int count = 0;
        while (resultSet.next()) {
            String province = resultSet.getString("province");
            provinces.add(province);
            sb.append("," + province);
            sb.append(",");
            sb.append("," );

            header1.append("彩铃,");
            header1.append("振铃,");
            header1.append("歌曲下载,");
        }
        header1.append("\n");
        sb.append("\n");
        sb.append(header1.toString());


        Iterator iter = all.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String sname = (String) entry.getKey();
            sb.append(generateLine(sname, map1, map2, map3));
        }



       /* Iterator iterator = map1.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();

            String sname = (String) entry.getKey();
            sname = sname.split("\\|\\|")[0];


            Map subMap = (Map) entry.getValue();

            sb.append(sname + ",");
            for (int i = 0; i < provinces.size(); i++) {
                String province = (String) provinces.get(i);
                Object download = subMap.get(province);
                if (download == null) {
                    sb.append(",");
                } else {
                    sb.append(download + ",");
                    count += Integer.parseInt(download.toString());
                }

                sb.append(",");
                sb.append(",");

            }

            int dbCount = getCount(sname, false);
            if (dbCount != count) {
                String name = sname;
                if (sname.indexOf("(") != -1) {
                    dbCount = getCount(sname, true);
                }

                if (dbCount != count) {
                    System.out.println(name + ":" + dbCount + "," + count);
                }


            }
            count = 0;

            sb.append("\n");
        }*/
        JdbcUtils.free(resultSet, preparedStatement, connection);

        //System.out.println("count=" + count);
        return sb.toString();
    }

    private static String generateLine(String sname, Map map1, Map map2, Map map3) throws SQLException {
        StringBuffer sb = new StringBuffer(sname + ",");
        Map subMap1 = new HashMap();
        Map subMap2 = new HashMap();
        Map subMap3 = new HashMap();

        if (map1.get(sname) != null) {
            subMap1 = (Map) map1.get(sname);
        }
        if (map2.get(sname) != null) {
            subMap2 = (Map) map2.get(sname);
        }
        if (map3.get(sname) != null) {
            subMap3 = (Map) map3.get(sname);
        }

        List province = getProvince();
        int count = 0;
        for (int i = 0; i < province.size(); i++) {
            Object download = subMap1.get(province.get(i));
            if (download == null) {
                sb.append(",");
            } else {
                sb.append(download + ",");
                count += Integer.parseInt(download.toString());
            }

            download = subMap2.get(province.get(i));
            if (download == null) {
                sb.append(",");
            } else {
                sb.append(download + ",");
                count += Integer.parseInt(download.toString());
            }

            download = subMap3.get(province.get(i));
            if (download == null) {
                sb.append(",");
            } else {
                sb.append(download + ",");
                count += Integer.parseInt(download.toString());
            }
        }

        int dbCount = getCount(sname, false);
        if (dbCount != count) {
            String name = sname;
            if (sname.indexOf("(") != -1) {
                dbCount = getCount(sname, true);
            }
            if (dbCount != count) {
                System.out.println(name + ":" + dbCount + "," + count);
            }
        }

        sb.append("\n");
        return sb.toString();  //To change body of created methods use File | Settings | File Templates.
    }

    private static List getProvince() throws SQLException {
        List provinces = new ArrayList();
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select * from province order by province");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String province = resultSet.getString("province");
            provinces.add(province);
        }
        JdbcUtils.free(resultSet, preparedStatement, connection);

        return provinces;
    }

    /*
    * 生成省数据
    * */
    private static void generateProvince() throws SQLException {

        Connection connection = JdbcUtils.getConnection();
        connection.prepareStatement("delete from province").execute();
        PreparedStatement preparedStatement = connection.prepareStatement("insert province (province) select distinct province from songs order by province");
        preparedStatement.execute();


        JdbcUtils.free(null, preparedStatement, connection);
    }

    /*
    * 根据歌名，取得该歌的下载总量
    * */
    public static int getCount(String sname, boolean filterSonger) throws SQLException {
        Connection connection = JdbcUtils.getConnection();
        String sql = "select sum(download) as download from analyze_data where sname='" + sname + "'";
        if (filterSonger) {
            String songer = sname.substring(sname.lastIndexOf("(") + 1, sname.length()-1);
            sname = sname.substring(0, sname.lastIndexOf("("));
            sql = "select sum(download) as download from analyze_data where sname='" + sname + "' and songer='" + songer +"'";
        }
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next()) {
            return 0;
        }

        int count = resultSet.getInt("download");
        JdbcUtils.free(resultSet, preparedStatement, connection);

        return count;
    }
}
