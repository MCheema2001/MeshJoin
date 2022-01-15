import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;

import org.apache.commons.collections4.multimap.*;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.MultiValuedMap;
import java.util.concurrent.*;
import java.util.*;


public class DWProjectrr {
	
	static Connection connection_;
	
	static Connection takecredentials() throws SQLException{
		String userName = "root";
		String userPassword = "Musadac20.";
		String dbName = "ProjectDW";
		char changecreds = 'n';
		Scanner input = new Scanner(System.in);
		System.out.println("Do you want to change default credential for dbms? (y/n).");
		changecreds = input.next().charAt(0);
		
		if(changecreds == 'y') {
			System.out.print("Enter Username(root): ");
			userName = input.next();
			System.out.print("Enter userPassword: ");
			userPassword = input.next();
			System.out.print("Enter dbName: ");
			dbName = input.next();
		}
		try {
			Connection con=DriverManager.getConnection(  
					"jdbc:mysql://localhost:3306/"+dbName,userName,userPassword);
			System.out.println("Connection Made.");
			return con;
			
		}catch(SQLException e1){
			System.out.println("Error Making Connection.");
			System.exit(0);
			}
		
		return null;
	}
	
	static void maketables() throws SQLException{
		String maketable_query = "CREATE TABLE PRODUCTS (`PRODUCT_ID` VARCHAR(100) NOT NULL, `PRODUCT_NAME` VARCHAR(100) NOT NULL, `PRICE` DOUBLE NOT NULL, CONSTRAINT PRODUCT_PK PRIMARY KEY (PRODUCT_ID))";
		Statement stmt_for_updating = connection_.createStatement();
		stmt_for_updating.executeUpdate("DROP TABLE IF EXISTS PRODUCTS, CUSTOMERS, STORES, SUPPLIERS, FACT_TABLE, DATES");
		stmt_for_updating.executeUpdate(maketable_query);
		maketable_query = "CREATE TABLE CUSTOMERS (`CUSTOMER_ID` VARCHAR(100) NOT NULL, `CUSTOMER_NAME` VARCHAR(100) NOT NULL, CONSTRAINT CUSTOMER_PK PRIMARY KEY (CUSTOMER_ID))";
		stmt_for_updating.executeUpdate(maketable_query);
		maketable_query = "CREATE TABLE STORES (`STORE_ID` VARCHAR(100) NOT NULL, `STORE_NAME` VARCHAR(100) NOT NULL, CONSTRAINT STORE_PK PRIMARY KEY (STORE_ID))";
		stmt_for_updating.executeUpdate(maketable_query);
		maketable_query = "CREATE TABLE SUPPLIERS (`SUPPLIERS_ID` VARCHAR(100) NOT NULL, `SUPPLIERS_NAME` VARCHAR(100) NOT NULL, CONSTRAINT SUPPLIERS_PK PRIMARY KEY (SUPPLIERS_ID))";
		stmt_for_updating.executeUpdate(maketable_query);
		maketable_query = "CREATE TABLE DATES (`DATE_ID` VARCHAR(100) NOT NULL, `DAY` VARCHAR(100) NOT NULL,`WEEK` VARCHAR(100) NOT NULL,`MONTH` VARCHAR(100) NOT NULL,`QUATER` VARCHAR(100) NOT NULL,`YEAR` VARCHAR(100) NOT NULL, CONSTRAINT DATE_PK PRIMARY KEY (DATE_ID))";
		stmt_for_updating.executeUpdate(maketable_query);
		maketable_query = "CREATE TABLE FACT_TABLE (`TRANSACTION_ID` VARCHAR(100) PRIMARY KEY, `PRODUCT_ID` VARCHAR(100) ,"
				+ " `CUSTOMER_ID` VARCHAR(100) , `SUPPLIERS_ID` VARCHAR(100) ,"
				+ " `STORE_ID` VARCHAR(100) , `QUANTITY` VARCHAR(100) NOT NULL,"
				+ "`DATE_ID` VARCHAR(100), `SUM` DOUBLE,"
				+ "FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID),FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID), "
				+ "FOREIGN KEY (SUPPLIERS_ID) REFERENCES SUPPLIERS(SUPPLIERS_ID), FOREIGN KEY (STORE_ID) REFERENCES STORES(STORE_ID), FOREIGN KEY (DATE_ID) REFERENCES DATES(DATE_ID))";
		stmt_for_updating.executeUpdate(maketable_query);
	}
	
	static void myOlap() {
		
	}
	
	public static int getWeekNum(String input) {
	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-M-dd");   
	    LocalDate date = LocalDate.parse(input, formatter);                     
	    WeekFields wf = WeekFields.of(Locale.getDefault()) ;                    
	    TemporalField weekNum = wf.weekOfWeekBasedYear();                     
	    int week = Integer.parseInt(String.format("%02d",date.get(weekNum)));   
	    return week;
	}
	
	static void MeshJoin(int stream_size, int master_size) throws SQLException{
		int counter_limit_1 = 0;
		int counter_limit_2 = 10;
		MultiValuedMap<String,Map<String,String>> map_table = new ArrayListValuedHashMap<>();
		int block_size = 10;
		int intodw = 0;
		Statement stmt_for_getting =  connection_.createStatement(); 
		Statement stmt_for_updating = connection_.createStatement();
		Statement stmt_for_getting_loop = connection_.createStatement();
		
		List<Map<String,String>> data = new ArrayList <Map<String,String>>();
		ArrayBlockingQueue<List<Map<String,String>>> queue = new ArrayBlockingQueue<List<Map<String,String>>>(block_size);
		int counter = 0;
		String myQuery = "";
		ResultSet rs;
		int upper_count_1 = 0;
		int upper_count_2 = 50;
		for(;;) {
			if(counter_limit_1 == master_size) {
				counter_limit_1 = 0;
			}
			myQuery = "SELECT * FROM TRANSACTIONS " + " LIMIT " + upper_count_1 + ", " + upper_count_2;
			rs = stmt_for_getting.executeQuery(myQuery);
			List<Map<String,String>> stream = new ArrayList<Map<String,String>>();
			while(rs.next()) {
				Map<String,String> data_temp = new HashMap<String,String>();
				data_temp.put("PRODUCT_ID", rs.getString("PRODUCT_ID"));
				data_temp.put("TRANSACTION_ID", rs.getString("TRANSACTION_ID"));
				data_temp.put("CUSTOMER_ID", rs.getString("CUSTOMER_ID"));
				data_temp.put("CUSTOMER_NAME", rs.getString("CUSTOMER_NAME"));
				data_temp.put("STORE_ID", rs.getString("STORE_ID"));
				data_temp.put("STORE_NAME", rs.getString("STORE_NAME"));
				data_temp.put("T_DATE", rs.getString("T_DATE"));
				data_temp.put("QUANTITY", rs.getString("QUANTITY"));
				data.add(data_temp);
				stream.add(data_temp);
				map_table.put(data_temp.get("PRODUCT_ID"), data_temp);
			}
			if(queue.size() >= 10) {
				for(Map<String, String>x:queue.poll()) {
					map_table.removeMapping(x.get("PRODUCT_ID"), x);
				}
			}
			queue.add(stream);
			myQuery = "SELECT * FROM MASTERDATA LIMIT " + counter_limit_1 + ", " + counter_limit_2;
			rs = stmt_for_getting_loop.executeQuery(myQuery);
			while(rs.next()) {
				Map<String,String> data_temp = new HashMap<String,String>();
				data_temp.put("PRODUCT_ID", rs.getString("PRODUCT_ID"));
				data_temp.put("PRODUCT_NAME", rs.getString("PRODUCT_NAME"));
				data_temp.put("SUPPLIER_ID", rs.getString("SUPPLIER_ID"));
				data_temp.put("SUPPLIER_NAME", rs.getString("SUPPLIER_NAME"));
				data_temp.put("PRICE", rs.getString("PRICE"));
				for(Map<String,String> temp_i:map_table.get(rs.getString("PRODUCT_ID"))) {
					String [] date_temp = temp_i.get("T_DATE").split("-");
					String supplier_id = data_temp.get("SUPPLIER_ID");
					double price_temp = Double.parseDouble(data_temp.get("PRICE"));
					double quantity_temp = Double.parseDouble(temp_i.get("QUANTITY"));
					String price_quantity_product = String.format("%.02f", price_temp * quantity_temp);
					String supplier_name = data_temp.get("SUPPLIER_NAME").replace("'", "");
					
					// Insertiion here into Schema Table should be present 
					myQuery = "SELECT * from PRODUCTS WHERE PRODUCT_ID = '" + data_temp.get("PRODUCT_ID") + "'";
					ResultSet qrs = stmt_for_getting.executeQuery(myQuery);
					if(!qrs.next()) {
						myQuery = "INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRICE) VALUES ('" 
								  + data_temp.get("PRODUCT_ID") + "', '" +  data_temp.get("PRODUCT_NAME") + "', '" + data_temp.get("PRICE") + "')";
						stmt_for_updating.executeUpdate(myQuery);
					}
					myQuery = "SELECT * from CUSTOMERS WHERE CUSTOMER_ID = '" + temp_i.get("CUSTOMER_ID") + "'";;
					qrs = stmt_for_getting.executeQuery(myQuery);
					if(!qrs.next()) {
						myQuery = "INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME) VALUES ('" +  temp_i.get("CUSTOMER_ID") + "', '" + temp_i.get("CUSTOMER_NAME") + "')";
						stmt_for_updating.executeUpdate(myQuery);
					}
					myQuery = "SELECT * from STORES WHERE STORE_ID = '" + temp_i.get("STORE_ID") + "'";
					qrs = stmt_for_getting.executeQuery(myQuery);
					if(!qrs.next()) {
						myQuery = "INSERT INTO STORES (STORE_ID, STORE_NAME) VALUES ('" +  temp_i.get("STORE_ID") + "', '" + temp_i.get("STORE_NAME") + "')";
						stmt_for_updating.executeUpdate(myQuery);
					}
					myQuery = "SELECT * from SUPPLIERS WHERE SUPPLIERS_ID = '" + supplier_id + "'";
					qrs = stmt_for_getting.executeQuery(myQuery);
					if(!qrs.next()) {
						myQuery = "INSERT INTO SUPPLIERS (SUPPLIERS_ID, SUPPLIERS_NAME) VALUES ('" +  supplier_id + "', '" + supplier_name + "')";
						stmt_for_updating.executeUpdate(myQuery);
					}
					myQuery = "SELECT * from DATES WHERE DATE_ID = '" + temp_i.get("T_DATE") + "'";
					qrs = stmt_for_getting.executeQuery(myQuery);
					if(!qrs.next()) {
						int quater = (Integer.parseInt(date_temp[1])/4)+1;
						myQuery = "INSERT INTO DATES (DATE_ID, DAY, WEEK, MONTH, QUATER, YEAR) VALUES ('"+ temp_i.get("T_DATE") +"', '" + date_temp[2] + "', '"+getWeekNum(temp_i.get("T_DATE"))+ "', '" +  date_temp[1] + "', '"+Integer.toString(quater)+ "', '" +  date_temp[0] +"')";
						stmt_for_updating.executeUpdate(myQuery);
					}
					myQuery = "INSERT INTO FACT_TABLE (TRANSACTION_ID, PRODUCT_ID, CUSTOMER_ID, STORE_ID, SUPPLIERS_ID, DATE_ID,QUANTITY, SUM) "+ "VALUES "+ "('" + temp_i.get("TRANSACTION_ID") + "', '" + temp_i.get("PRODUCT_ID") +  "', '" + temp_i.get("CUSTOMER_ID") + "', '" + temp_i.get("STORE_ID") + "', '" + data_temp.get("SUPPLIER_ID") +"', '" + temp_i.get("T_DATE")  +"', '" + temp_i.get("QUANTITY")  + "', '" + price_quantity_product + "')";
					stmt_for_updating.executeUpdate(myQuery);
					
					intodw++;
				}
			}
			upper_count_1 += 50;
			if(intodw == stream_size) {
				break;
			}
			counter_limit_1 += 10;
		}
		
	}
	
	public static void main(String[] args) {
		try {
			connection_ = takecredentials();
			char maket = 'y';
			Scanner input = new Scanner(System.in);
			System.out.println("Do you want to make Tables Star Schema Automatically through Java(y/n)?");
			maket = input.next().charAt(0);
			if(maket == 'y') {
				maketables();
			}
			Statement stmt=connection_.createStatement();  
			ResultSet master_data=stmt.executeQuery("select * from MASTERDATA");  
			int master_data_size = 0;
			while(master_data.next()) {
				master_data_size ++;
			}
			ResultSet stream=stmt.executeQuery("select * from TRANSACTIONS"); 
			int transaction_size = 0;
			while(stream.next()) {
				transaction_size ++;
			}
			System.out.println("MeshJoin Working!");
			MeshJoin(transaction_size,master_data_size);
			System.out.println("MeshJoin Ended!");
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}   
}

