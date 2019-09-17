package lcd.tp01;

import java.sql.*;
import java.io.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Collection;

class JDBCModel implements IModel {

	private Connection connection = null;
	
	//Modification des noms des tables de la base de doonée
	private static final String[] tableNames = { "MOVIE_mhd", "PEOPLE_mhd", "DIRECTOR_mhd", "ROLE_mhd" };
	
	//La fonction d'initialisation de la connexion
	JDBCModel(String username, String password, String base) throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresqlDriver");
		connection = DriverManager.getConnection("jdbc:postgresql://tp-postgres/");
	}
	
	public String[] getTableNames() {
		return tableNames;
	}

	// Ferme explicitement la connexion.
	public void close() throws Exception {
		/*
		 * Si la variable de connexion n'est pas null on ferme la connexion puis on la met à null 
		 */
		if (connection != null) {
			connection.close();
			connection = null;
			
		}

	}

	// Appelé lors de la destruction de l'objet par le GC.
	protected void finalize() throws Throwable {
		this.close();
	}

	public void initialize() throws SQLException {
		if (connection != null) {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("Drop Table if exists MOVIE_mhd cascade");
			stmt.executeUpdate("Drop Table if exists PEOPLE_mhd cascade");
			stmt.executeUpdate("Drop Table if exists DIRECTOR_mhd cascade");
			stmt.executeUpdate("Drop Table if exists ROLE_mhd cascade");
			stmt.executeUpdate("CREATE TABLE PEOPLE_mhd (pid INTEGER, firstname VARCHAR(30),"
								+ "lastname VARCHAR(30), PRIMARY KEY(pid))");
			stmt.executeUpdate("CREATE TABLE MOVIE_mhd (mid INTEGER, title VARCHAR(90) NOT NULL,"
								+ "year INTEGER NOT NULL,                    "
								+ "runtime INTEGER NOT NULL, rank INTEGER NOT NULL, PRIMARY KEY (mid))");
			
			stmt.executeUpdate("CREATE TABLE ROLE_mhd (mid INTEGER, pid INTEGER, name VARCHAR(70), "
					+ "PRIMARY KEY(mid, pid, name), FOREIGN KEY (mid) REFERENCES MOVIE_mhd, "
					+ "FOREIGN KEY (pid) REFERENCES PEOPLE_mhd)");
			
			stmt.executeUpdate("CREATE TABLE DIRECTOR_mhd (mid INTEGER, pid INTEGER, PRIMARY KEY (mid, pid), "
							+ "FOREIGN KEY (mid) REFERENCES MOVIE_mhd,"
							+ "FOREIGN KEY (pid) REFERENCES PEOPLE_mhd)");
		}

	}

	private void fillMovie(BufferedReader r) throws SQLException, IOException {
		String values;
		Statement stmt = connection.createStatement();
		int n = 0;
		String[] v;
		while((values = r.readLine())!=null) {
			v = values.split(";", 5);
			stmt.addBatch("INSERT INTO MOVIE_mhd VALUES("+v[0]+", '"
					+v[1].replace("'", "''")+"', "
					+v[2]+","
					+v[3]+", "
					+v[4]+")");
		}
		
		for(int i : stmt.executeBatch())
			n+=i;
		
		System.out.println("Insertion terminés "+n+" Lignes inserée dans la table MOVIES");
	}

	private void fillPeople(BufferedReader r) throws SQLException, IOException {
		String values;
		Statement stmt = connection.createStatement();
		int n=0;
		String[] v;
		while((values = r.readLine())!=null) {
			v = values.split(";",3);
			stmt.addBatch("INSET INTO PEOPLE_mhd VALUES("+v[0]+", '"
					+v[1].replace("'","''")+"','"
					+v[2].replace("'", "''")+"')");
		}
		for(int i : stmt.executeBatch()) 

            n+=i; 

        System.out.println("Insertion terminée "+n+" Lignes insérées dans la table PEOPLE"); 
	}

	private void fillDirector(BufferedReader r) throws SQLException, IOException {
		String values;
		Statement stmt = connection.createStatement();
		int n = 0;
		String[] v;
		while ((values = r.readLine()) != null) {
			v = values.split(";", 2);
			stmt.addBatch("INSERT INTO DIRECTOR_mhd VALUES (" + v[0] + ", " + v[1] + ")");
		}
		;

		for (int i : stmt.executeBatch())
			n += i;

		System.out.println("Insertion terminée "+n+" Lignes insérées dans la table DIRECTOR"); 

	}

	private void fillRole(BufferedReader r) throws SQLException, IOException {
		String values;
		Statement stmt = connection.createStatement();
		int n = 0;
		String[] v;
		while ((values = r.readLine()) != null) {
			v = values.split(";", 3);
			stmt.addBatch("INSERT INTO ROLE_mhd VALUES (" + v[0] + ", " + v[1] + ", '" + v[2].replace("'", "''") + "')");
		}
		;

		for (int i : stmt.executeBatch())
			n += i;

		System.out.println("Insertion terminée "+n+" Lignes insérées dans la table ROLE"); 
	}

	public void fillTables(Map<String, File> files) throws Exception {
		if (connection != null) {
			try {
				/*
				 * CADEAU, BIEN LIRE LE CODE MAIS RIEN À COMPLÉTER
				 */
				connection.setAutoCommit(false);
				File f;
				f = files.get("MOVIE_mhd");
				if (f == null)
					throw new Exception();
				fillMovie(new BufferedReader(new FileReader(f)));

				f = files.get("PEOPLE_mhd");
				if (f == null)
					throw new Exception();
				fillPeople(new BufferedReader(new FileReader(f)));

				f = files.get("DIRECTOR_mhd");
				if (f == null)
					throw new Exception();
				fillDirector(new BufferedReader(new FileReader(f)));

				f = files.get("ROLE_mhd");
				if (f == null)
					throw new Exception();
				fillRole(new BufferedReader(new FileReader(f)));

				connection.commit();

			} catch (Exception e) {
				connection.rollback();
				close();
				throw (e);
			}

		}
	}

	public Collection<String> query(String pattern) throws Exception {

		if (connection != null) {
			//Pour le patern Like
			pattern = "'%" + pattern + "%'";
			
			//On crée un vector pour parcourir le resultat, pouvait mettre autre chose List, tableau, ...
			Vector<String> v = new Vector<String>();
			
			Statement stmt = connection.createStatement();
			
			String sql = "Select title, year from movie_mhd where title like "+pattern;
			ResultSet  res = stmt.executeQuery(sql);
			while(res.next()) {
				String s = res.getString(1);
				v.add(s);
			}
			/*
			 * À COMPLÉTER. ÉCRIRE DES REQUÊTES POUR REMPLIR v
			 * AFIN QU'IL CONTIENNE DES CHAINES DE LA FORME
			   Titre, Année, durée, Real1, …, Realn, Acteur 1 : Role 1, … Acteur m: Role m
			 */
			sql  = "SELECT m.title, p.firstname, p.lastname, r.name FROM movie m, role r, people p"
					+ " WHERE m.mid = r.mid AND p.pid = r.pid AND"
					+ " (p.firstname LIKE " + pattern
					+ " OR p.lastname LIKE " + pattern
					+ " OR r.name LIKE " + pattern
					+ ")";
			res = stmt.executeQuery(sql);
			while (res.next()) {
				String s = res.getString(1);
				s += ", " + res.getString(2);
				s += ", " + res.getString(3);
				s += ", " + res.getString(4);
				v.add(s);
			}
			return v;

		} else
			throw new Exception();

	}
	
	public static void main(String[] args) {
		try {
			IModel dbm = new JDBCModel("kim", "kim", "kim");
			dbm.initialize();
			Map<String, File> files = new TreeMap<String, File>();
			files.put("MOVIE_mhd", new File("src/main/resources/movie.txt"));
			files.put("PEOPLE_mhd", new File("src/main/resources/people.txt"));
			files.put("DIRECTOR_mhd", new File("src/main/resources/director.txt"));
			files.put("ROLE_mhd", new File("src/main/resources/role.txt"));

			dbm.fillTables(files);
			dbm.close();

		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
