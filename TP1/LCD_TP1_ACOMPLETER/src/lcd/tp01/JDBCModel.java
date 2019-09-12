package lcd.tp01;

import java.sql.*;
import java.io.*;
import java.util.Map;
import java.util.Vector;
import java.util.Collection;

class JDBCModel implements IModel {

	private Connection connection = null;
	private static final String[] tableNames = { "MOVIE_mhd", "PEOPLE_mhd", "DIRECTOR_mhd", "ROLE_mhd" };

	JDBCModel(String username, String password, String base) throws SQLException, ClassNotFoundException {

		/*
		 * À COMPLETER CONNEXION À LA BASE POSTGRESQL SUR LA MACHINE tp-postgres
		 */

	}

	public String[] getTableNames() {
		return tableNames;
	}

	// Ferme explicitement la connexion.
	public void close() throws Exception {
		if (connection != null) {
			/*
			 * À COMPLETER FERMER LA CONNEXION ET INITIALISER À NULL
			 */
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
			/*
			 * À COMPLÉTER - DÉTRUIRE LES TABLES EXISTANTES - CRÉER LES TABLES
			 * AVEC LE SCHEMA DEMANDÉ
			 */
		}

	}

	private void fillMovie(BufferedReader r) throws SQLException, IOException {
		Statement stmt = connection.createStatement();
		String line = null;
		while((line = r.readLine()) != null) {
			String[] fields = line.split(";");
			String insert = "insert into MOVIE_mhd values("
					+ fields[0]+ ","
					+ fields[1]+ ","
					+ fields[2]+ ","
					+ fields[3]+ ","
					+ fields[4]+ ";"
					+ ")";
		}

		/*
		 * À COMPLÉTER : lire 'r' ligne à ligne et remplir la table MOVIE. On
		 * pourra utiliser String.split() pour séparer selon des ';'.
		 */

	}

	private void fillPeople(BufferedReader r) throws SQLException, IOException {
		/*
		 * À COMPLÉTER : lire 'r' ligne à ligne et remplir la table MOVIE. On
		 * pourra utiliser String.split() pour séparer selon des ';'.
		 */
	}

	private void fillDirector(BufferedReader r) throws SQLException, IOException {
		/*
		 * À COMPLÉTER : lire 'r' ligne à ligne et remplir la table MOVIE. On
		 * pourra utiliser String.split() pour séparer selon des ';'.
		 */

	}

	private void fillRole(BufferedReader r) throws SQLException, IOException {
		/*
		 * À COMPLÉTER : lire 'r' ligne à ligne et remplir la table MOVIE. On
		 * pourra utiliser String.split() pour séparer selon des ';'.
		 */
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
			pattern = "'%" + pattern + "%'";
			Vector<String> v = new Vector<String>();

			/*
			 * À COMPLÉTER. ÉCRIRE DES REQUÊTES POUR REMPLIR v
			 * AFIN QU'IL CONTIENNE DES CHAINES DE LA FORME
			   Titre, Année, durée, Real1, …, Realn, Acteur 1 : Role 1, … Acteur m: Role m
			 */

			return v;

		} else
			throw new Exception();

	}
}
