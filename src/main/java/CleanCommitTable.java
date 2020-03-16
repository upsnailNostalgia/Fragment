import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @ProjectName: Fragment
 * @Package: PACKAGE_NAME
 * @ClassName: CleanCommitTable
 * @Description:
 * @Author: bruce
 * @CreateDate: 2020/3/16 10:33
 * @Version: 1.0
 */
public class CleanCommitTable {
    public static java.sql.Connection connectDatabase() throws SQLException {
        String url = "jdbc:mysql://10.131.252.160:3306/issueTracker?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        //String url = "jdbc:mysql://localhost:3306/issuetracker?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        String user = "root";
        //用户名
        String password = "root";
        //密码

        Driver driver = new com.mysql.cj.jdbc.Driver();
        //新版本
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        Connection conn = driver.connect(url, props);
        return conn;
    }

    public static List<String> getIdFromMysql(String str_sql) {
        List<String> result = new ArrayList<String>();
        try {
            Connection connection = connectDatabase();
            PreparedStatement preparedStatement;
            String sql_select = str_sql;
            preparedStatement = connection.prepareStatement(sql_select);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                result.add(resultSet.getString(1));
            }
            preparedStatement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void deleteCommit(String strDel, List<String> list) {
        try {
            Connection connection = connectDatabase();
            PreparedStatement preparedStatement;
            for (String repoId : list) {
                preparedStatement = connection.prepareStatement(strDel+"'"+repoId+"'");
                preparedStatement.executeUpdate();
                preparedStatement.close();
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //首先拿到repository表中的所有uuid的集合list1
        String strSql1 = "select uuid from repository";
        List<String> list1 = getIdFromMysql(strSql1);
        System.out.println("list1为：");
        list1.stream().forEach(System.out::println);
        //然后拿到commit表中的所有repo_id（distinct）的集合list2
        String strSql2 = "select DISTINCT repo_id from commit";
        List<String> list2 = getIdFromMysql(strSql2);
        System.out.println("list2为：");
        list2.stream().forEach(System.out::println);
        //得到list2-list1=list3
        List<String> list3 = list2.stream().filter(item->!list1.contains(item))
                                .collect(Collectors.toList());
        System.out.println("list3为：");
        list3.stream().forEach(System.out::println);
        //删除commit表中repoid在list3中的记录
        String strDel = "delete from commit where repo_id = ";
        deleteCommit(strDel,list3);
    }
}
