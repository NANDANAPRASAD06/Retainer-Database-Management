import java.sql.*; 
import java.util.*;
import java.io.*;
import java.io.FileReader;
public class MyTest {

	public static void main(String[] args) throws FileNotFoundException{
		try {
		         Connection conn = DriverManager.getConnection(
		               "jdbc:mysql://localhost:3306", "root", "1234");

		         Statement stmt = conn.createStatement();
		         stmt.executeUpdate("create database if not exists homework4db");
		         stmt.executeUpdate("use homework4db");
		         String sql = "CREATE TABLE employee " +
		                   "(eid INTEGER not NULL, " + 
		                   " name VARCHAR(20), " + 
		                   " salary INTEGER, " + 
		                   " PRIMARY KEY ( eid ))"; 
		         stmt.executeUpdate("drop table if exists employee");
		         stmt.executeUpdate(sql);
		         System.out.println("Created employee table in given database...");
		         stmt.executeUpdate("drop table if exists supervisor");
		         String sql1 = "CREATE TABLE supervisor " +
		                   "(eid int references employee(eid), " + 
		                   " sid int references employee(eid)," +  
		                   " PRIMARY KEY (eid))";
		         stmt.executeUpdate(sql1);
		         System.out.println("Created supervisor table in given database...");
		         Integer eid[] = new Integer[] {10,20,30,40,50,60,70,80,90,100,110,120};
		 		 String name[] = new String[] {"Nandana", "Namrata", "Abhijeet", "Dhanvin", "Amogh", "Abhilash", "Ram", "Raji", "Apurva", "Swathi", "Meghana", "kavitha"};
		 		 Integer salary[] = new Integer[] {160000, 400000, 350000, 400000, 100000, 450000, 200000, 530000,  200000, 220000, 430000, 100000};
		 		 PreparedStatement ps = conn.prepareStatement(
		 	            "INSERT INTO employee VALUES (?,?,?)");
		 			for (int i = 0; i < 12; ++i) 
		 			{
		 				ps.setInt(1, eid[i]);
		 				ps.setString(2, name[i]);
		 				ps.setInt(3, salary[i]);
		 				ps.executeUpdate();
		 			}
		 		Integer employeeeid[] = new Integer[] {20, 50, 30, 40, 60, 10, 70, 80, 90, 100, 110};
		 		Integer supervisorsid[] = new Integer[] {10, 90, 100, 30, 100, 120, 40, 90, 110, 120, 20};
		 		PreparedStatement ps1 = conn.prepareStatement(
		 	            "INSERT INTO supervisor VALUES (?,?)");
		 			for (int i = 0; i < 11; ++i) 
		 			{
		 				ps1.setInt(1, employeeeid[i]);
		 				ps1.setInt(2, supervisorsid[i]);
		 				ps1.executeUpdate();
		 			}
		         BufferedReader linereader;
		         try
		         {
		         linereader = new BufferedReader(new FileReader("C:\\Users\\NANDANA S PRASAD\\OneDrive\\Desktop\\MS\\First_Sem2019\\DatabaseSystems\\ASSIGNMENTS\\HW4\\transfile.txt"));
		         String line = linereader.readLine();
		         while(line != null)
		         {
		        	 String[] a = line.split(" ");
		        	 Integer Trans_code = Integer.parseInt(a[0]);
				     System.out.println("transaction_code " +Trans_code+ "\n");
				     if (Trans_code == 1)
				     {
				    	 Integer eid_to_delete =  Integer.parseInt(a[1]);
				    	 PreparedStatement ps9 = null;
				    	 ps9 = conn.prepareStatement("SELECT eid FROM employee WHERE eid=?");
				    	 ps9.setInt(1, eid_to_delete);
				    	 ResultSet rset =ps9.executeQuery();
				    	 if(rset.next())
				    	 {
					    	 if(eid_to_delete.toString() != null)
						    	
					                    ps9 = conn.prepareStatement("DELETE FROM employee WHERE eid=?");
					                    ps9.setInt(1, eid_to_delete);               
					                    Integer r =ps9.executeUpdate();
					                    PreparedStatement PS1 = null;
					                    PS1 = conn.prepareStatement("UPDATE supervisor SET sid = ? WHERE sid=?");
					                    PS1.setString(1, null); 
					                    PS1.setInt(2, eid_to_delete);               
					                    Integer r1 =PS1.executeUpdate();

					                    if(r!=0)
					                           System.out.println("Done! Successfully deleted an employee record ");
					             
				    	 }
				    	 else
				    	 {
				    		 System.out.println("Error");
				    	 }
				    		 
				     }
				     if (Trans_code == 2)
					     {
					    	 Integer eid_to_insert =  Integer.parseInt(a[1]);
					    	 Integer eid_salary =  Integer.parseInt(a[3]);
					    	 PreparedStatement ps3 = null;
					    	 ps3 = conn.prepareStatement("INSERT into employee values(?, ?, ?)");
					    	 ps3.setInt(1, eid_to_insert);
					    	 ps3.setString(2, a[2]); 
					    	 ps3.setInt(3, eid_salary);
					    	 
					    	 Integer rset =ps3.executeUpdate();
					    	 Integer eid_of_sid =  Integer.parseInt(a[4]);
					    	 PreparedStatement ps4 = null;
					    	 ps4 = conn.prepareStatement("SELECT eid FROM employee WHERE eid=?");
					    	 ps4.setInt(1, eid_of_sid);
					    	 ResultSet rset1 =ps4.executeQuery();
					    	 if(rset1.next())
					    	 {
					    		 PreparedStatement ps5 = null;
						    	 ps5 = conn.prepareStatement("INSERT into supervisor values(?, ?)");
						    	 ps5.setInt(1, eid_to_insert);
						    	  
						    	 ps5.setInt(2, eid_of_sid);
						   
						    	 Integer rset2 =ps5.executeUpdate();
						    	 if(rset2!=0)
						    	 {
						    		 System.out.println("Done inserting tuple into employee and supervisor table");
						    	 }
					    	 }
					    	 else
					    	 {
					    		 System.out.println("Error as given SID doesn't existin employee table, so inserting (eid,NULL) in supervisor table");
					    		 PreparedStatement ps6 = null;
						    	 ps6 = conn.prepareStatement("INSERT into supervisor values(?, ?)");
						    	 ps6.setInt(1, eid_to_insert);
						    	  
						    	 ps6.setString(2, null);
						   
						    	 Integer rset3 =ps6.executeUpdate();
					    	 }
		                }
				     if (Trans_code == 3)
				     {
				    	 
				    	 Integer eid_to_update =  Integer.parseInt(a[1]);
				    	 int length = a.length;
				    	 	if (length > 2)
				    	 	{
				    	 		Integer sid_to_update =  Integer.parseInt(a[2]);
				    			 PreparedStatement ps2 = null;
				                 ps2 = conn.prepareStatement("UPDATE supervisor SET sid = ? WHERE eid=?");
				                 ps2.setInt(1, sid_to_update); 
				                 ps2.setInt(2, eid_to_update);               
				                 Integer r1 =ps2.executeUpdate();
				    	 	}
				    	 	else
				    		{
				    			 
				    			PreparedStatement ps2 = null;
				                 ps2 = conn.prepareStatement("UPDATE supervisor SET sid = ? WHERE eid=?");
				                 ps2.setString(1, null); 
				                 ps2.setInt(2, eid_to_update);               
				                 Integer r1 =ps2.executeUpdate();	
				    		}
				    	   
				    	 
				     }
				     else if (Trans_code == 4)
				     {
	
				    	 String qset = "SELECT avg(salary)FROM employee";
				    	 ResultSet rset =stmt.executeQuery(qset);
				    	 while(rset.next())
				    	 {
				    		 Integer avg_sal = rset.getInt(1);
				    		 System.out.println("AVG_SALARY is : " +avg_sal+ "\n");
				    	 }
				     }
				     if (Trans_code == 5)
				     {
				    	 Integer sid_value =  Integer.parseInt(a[1]);
				    	 PreparedStatement ps8 = null;
				    	 ps8 = conn.prepareStatement("SELECT eid FROM supervisor WHERE sid=?");
				    	 ps8.setInt(1, sid_value);
				    	 ResultSet rs8 =ps8.executeQuery();
				    	 List<Integer> eid_list =  new ArrayList<Integer>();	 
				    	 while (rs8.next())
				    	 {
				    		 Integer supervisor_list[] = new Integer[] {rs8.getInt(1)};
				    		 for(int k=0; k < supervisor_list.length; ++k)
				    		 {
				    			 eid_list.add(supervisor_list[k]);
				    		 }
				    	 }
				    	 if(eid_list !=  null)
				    	 {
				    		 ps8 = conn.prepareStatement("SELECT eid FROM supervisor WHERE sid=?");
				    		 for(int i=0; i < eid_list.size(); i++)
				    		 {
				    			 ps8.setInt(1, eid_list.get(i));
						    	 ResultSet rs9 =ps8.executeQuery();
						    	 while(rs9.next())
						    	 {
						    		 if(eid_list.contains(rs9.getInt(1)))
						    		 {
						    			 continue;
						    		 }
						    		 else
						    		 {
						    			 eid_list.add(rs9.getInt(1));
						    		 }
						    	 }
				    		 }					    	
				    	  }
				    	 
				    	 ps8 = conn.prepareStatement("SELECT name FROM employee WHERE eid=?");
			    		 for(int i=0; i < eid_list.size(); i++)
			    		 {
			    			 ps8.setInt(1, eid_list.get(i));
					    	 ResultSet rs10 =ps8.executeQuery();
					    	 while(rs10.next())
					    	 {
				    	        System.out.print(rs10.getString(1) + "\n");
					    	 }
				    	 
				    	 }
				    		 
				     }
				     if (Trans_code == 6)
				     {
				    	 Integer sid_value =  Integer.parseInt(a[1]);
				    	 PreparedStatement ps8 = null;
				    	 ps8 = conn.prepareStatement("SELECT eid FROM supervisor WHERE sid=?");
				    	 ps8.setInt(1, sid_value);
				    	 ResultSet rs8 =ps8.executeQuery();
				    	 List<Integer> eid_list =  new ArrayList<Integer>();	
				    	 List<Integer> salary_list =  new ArrayList<Integer>();
				    	 while (rs8.next())
				    	 {
				    		 Integer supervisor_list[] = new Integer[] {rs8.getInt(1)};
				    		 for(int k=0; k < supervisor_list.length; ++k)
				    		 {
				    			 eid_list.add(supervisor_list[k]);
				    		 }
				    	 }
				    	 if(eid_list !=  null)
				    	 {
				    		 ps8 = conn.prepareStatement("SELECT eid FROM supervisor WHERE sid=?");
				    		 for(int i=0; i < eid_list.size(); i++)
				    		 {
				    			 ps8.setInt(1, eid_list.get(i));
						    	 ResultSet rs9 =ps8.executeQuery();
						    	 while(rs9.next())
						    	 {
						    		 if(eid_list.contains(rs9.getInt(1)))
						    		 {
						    			 continue;
						    		 }
						    		 else
						    		 {
						    			 eid_list.add(rs9.getInt(1));
						    		 }
						    	 }
				    		 }					    	
				    	  }
				    	 
				    	 ps8 = conn.prepareStatement("SELECT salary FROM employee WHERE eid=?");
				    	 int total_sal = 0;
				    	 for(int i=0; i < eid_list.size(); i++)
			    		 {
			    			 ps8.setInt(1, eid_list.get(i));
					    	 ResultSet rs11 =ps8.executeQuery();
					    	 while(rs11.next())
					    	 {
					    		 total_sal = total_sal + rs11.getInt(1);
					    	 }
				    	 }
			    		 int avg_sal = total_sal/eid_list.size();
			    		 System.out.print("  AVG SALARY of EMPLOYEES working directly or indirectly under 100 is " + avg_sal+ "\n\n");
	 
				     }
				    	 
				     
		        	 line = linereader.readLine();
		         }
		         linereader.close();
		         stmt.executeUpdate("drop table if exists employee");
		         stmt.executeUpdate("drop table if exists supervisor");
		         System.out.println("Dropped Employee table from given database..."+"\n");
		         System.out.println("Dropped Supervisor table from given database..."+"\n");
		         stmt.close();
		         conn.close();
		         }
		         catch (IOException er)
		 		{
		 			System.out.println("ERROR is : " +er+ "\n");
		 		}
		
		}
		catch (SQLException e)
		{
			System.out.println("ERROR is : " +e+ "\n");
		}
		
	}
}
	

