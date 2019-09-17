package lcd.tp01;

/*
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Map;
import java.util.Vector;
import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;*/
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.SetJoin;

import lcd.tp01.Movie;
import lcd.tp01.People;
import lcd.tp01.Role;

import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

@Entity
class Movie {
	@Id
	Long mid;
	@Column(length = 90, nullable = false)
	private String title;
	@Column(nullable = false)
	private int year;
	@Column(nullable = false)
	private int runtime;
	@Column(nullable = false)
	private int rank;

	@ManyToMany
	Set<People> directors = new HashSet<People>();

	@OneToMany
	Set<Role> actors = new HashSet<Role>();

	Movie() {
	};

	public Movie(Long mid, String title, int year, int runtime, int rank) {
		this.mid = mid;
		this.title = title;
		this.year = year;
		this.runtime = runtime;
		this.rank = rank;
	}

	public Movie(String[] line) {
		this(Long.parseLong(line[0]), line[1], Integer.parseInt(line[2]), Integer.parseInt(line[3]),
				Integer.parseInt(line[4]));

	}

	public String getTitle() {
		return this.title;
	}

	public int getYear() {
		return this.year;
	}

	public int getruntime() {
		return this.runtime;
	}

	public int getRank() {
		return this.rank;
	}

}


@Entity
class People {
	@Id
	Long pid;
	@Column(length = 30, nullable = false)
	String firstname;
	@Column(length = 30, nullable = false)
	String lastname;

	People() {
	}

	People(Long pid, String firstname, String lastname) {
		this.pid = pid;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	People(String[] line) {
		this(Long.parseLong(line[0]), line[1], line[2]);
	}

	String getFirstname() {
		return this.firstname;
	}

	String getLastname() {
		return this.lastname;
	}
}


@Entity
class Role {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	Long rid;
	@Column
	String name;

	@OneToOne
	People actor;

	Role() {
	};

	Role(String name, People actor) {
		this.name = name;
		this.actor = actor;
	}
}

class QueryResult {
	String title;
	int year;
	String firstname;
	String lastname;
	String role;

	public QueryResult(String title, int year, String firstname, String lastname, String role) {
		this.title = title;
		this.year = year;
		this.firstname = firstname;
		this.lastname = lastname;
		this.role = role;
	}
}

public class JPAModel implements IModel {

	private EntityManagerFactory emf;
	private EntityManager em;

	private static final String[] tableNames = { "MOVIE_mhd", "PEOPLE_mhd", "DIRECTOR_mhd", "ROLE_mhd" };

	JPAModel(String username, String password, String base) throws SQLException, ClassNotFoundException {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put("javax.persistence.jdbc.driver", "org.postgresql.Driver");
		config.put("javax.persistence.jdbc.user", username);
		config.put("javax.persistence.jdbc.password", password);
		config.put("javax.persistence.jdbc.url", base);

		//utilise le nom donné dans l'attribut 'name' de l'élément persistence-unit
		//de src/META-INF/persistence.xml
		emf = Persistence.createEntityManagerFactory("LCD_TP1", config);
		em = emf.createEntityManager();
	}

	public void initialize() throws Exception {
	};

	// Ferme explicitement la connexion.
	public void close() throws Exception {
		if (em != null) {
			em.close();
			emf.close();
			em = null;
			emf = null;
		}

	}

	protected void finalize() throws Throwable {
		this.close();
	}

	public String[] getTableNames() {
		return tableNames;
	}

	public void fillTables(Map<String, File> files) throws Exception {
		if (em != null && em.isOpen()) {
			File f;
			BufferedReader fr;
			String line;
			try {
			
			em.getTransaction().begin();
			
			
			f = files.get("MOVIE");
			if (f == null) throw new Exception();
			fr = new BufferedReader(new FileReader(f)); 
	
			
			while ((line = fr.readLine()) != null)
			{
				em.persist(new Movie(line.split(";",5)));
			}
			fr.close();
			
			f = files.get("PEOPLE");
			if (f == null) throw new Exception();
			fr = new BufferedReader(new FileReader(f));
			
			while ((line = fr.readLine()) != null)
			{
				em.persist(new People(line.split(";",3)));
			}
			fr.close();
			f = files.get("DIRECTOR");
			if (f == null) throw new Exception();
			fr = new BufferedReader(new FileReader(f));
			
			while ((line = fr.readLine()) != null)
			{
				String[] v = line.split(";", 2);
				Long mid = Long.parseLong(v[0]);
				Long pid = Long.parseLong(v[1]);
				em.find(Movie.class, mid).directors.add(em.find(People.class, pid));				
			}
			fr.close();
			f = files.get("ROLE");
			if (f == null) throw new Exception();
			fr = new BufferedReader(new FileReader(f));
			
			while ((line = fr.readLine()) != null)
			{
				String[] v = line.split(";", 3);
				Long mid = Long.parseLong(v[0]);
				Long pid = Long.parseLong(v[1]);
				String name = v[2];
				Role r = new Role(name, em.find(People.class, pid));
				em.persist(r);
				em.find(Movie.class, mid).actors.add(r);
			}
			fr.close();
			em.getTransaction().commit();
			
			} catch (Exception e) {
				System.err.println("HERE!");
				em.getTransaction().rollback();
				throw e;
			}
			
		} else {
			throw new Exception();
		}

	}

	public Collection<String> query(String pattern) throws Exception {
		Vector<String> res = new Vector<String>();

		/*
		 * À COMPLÉTER : Créer des requêtes 'CriteriaQuery' pour calculer le résultat demandé
		 */
		
		return res;

	}
	/*
	 public Collection<String> query(String pattern) throws Exception {

		pattern = "%" + pattern + "%";
		CriteriaBuilder b = em.getCriteriaBuilder();
		CriteriaQuery<QueryResult> q1 = b.createQuery(QueryResult.class);
		Root<Movie> movies = q1.from(Movie.class);
		SetJoin<Movie, People> jmp = movies.join(Movie_.directors);
		q1.where(b.or(b.like(movies.get(Movie_.title), pattern), b.like(jmp.get(People_.firstname), pattern),
				b.like(jmp.get(People_.lastname), pattern)));
		q1.select(b.construct(QueryResult.class, movies.get(Movie_.title), movies.get(Movie_.year),
				jmp.get(People_.firstname), jmp.get(People_.lastname), b.literal("#DIRECTOR#")));

		TypedQuery<QueryResult> tq1 = em.createQuery(q1);
		List<QueryResult> result = tq1.getResultList();

		CriteriaQuery<QueryResult> q2 = b.createQuery(QueryResult.class);
		movies = q2.from(Movie.class);
		SetJoin<Movie, Role> jmr = movies.join(Movie_.actors);
		q2.where(
				b.or(b.like(jmr.get(Role_.name), pattern), b.like(jmr.get(Role_.actor).get(People_.firstname), pattern),
						b.like(jmr.get(Role_.actor).get(People_.lastname), pattern)));

		q2.select(b.construct(QueryResult.class, movies.get(Movie_.title), movies.get(Movie_.year),
				jmr.get(Role_.actor).get(People_.firstname), jmr.get(Role_.actor).get(People_.lastname),
				jmr.get(Role_.name)));

		TypedQuery<QueryResult> tq2 = em.createQuery(q2);
		List<QueryResult> result2 = tq2.getResultList();
		result.addAll(result2);

		Vector<String> res = new Vector<String>();

		for (QueryResult line : result)
			res.add(line.title + ", " + line.year + ", " + line.firstname + ", " + line.lastname + ", " + line.role);
		return res;
	}*/


}
