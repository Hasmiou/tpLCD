package lcd.tp01;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Map;
import java.util.Vector;
import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;

@Entity
class Movie {
	/*
	 * À COMPLÉTER : définir les attributs de Movie avec les annotations
	 * adéquate
	 */

}

@Entity
class People {
	/*
	 * À COMPLÉTER : définir les attributs de People avec les annotations
	 * adéquate
	 */

}

@Entity
class Role {
	/*
	 * À COMPLÉTER : définir les attributs de People avec les annotations
	 * adéquate
	 */
}

public class JPAModel implements IModel {

	private EntityManagerFactory emf;
	private EntityManager em;

	private static final String[] tableNames = { "MOVIE", "PEOPLE", "DIRECTOR", "ROLE" };

	JPAModel(String username, String password, String base) throws SQLException, ClassNotFoundException {

		Map<String, Object> config = new HashMap<String, Object>();
		config.put("javax.persistence.jdbc.driver", "" /* À COMPLÉTER */);
		config.put("javax.persistence.jdbc.user", "" /* À COMPLÉTER */);
		config.put("javax.persistence.jdbc.password", "" /* À COMPLÉTER */);
		config.put("javax.persistence.jdbc.url", "" /* À COMPLÉTER */);

		//utilise le nom donné dans l'attribut 'name' de l'élément persistence-unit
		//de src/META-INF/persistence.xml
		emf = Persistence.createEntityManagerFactory("LCD_TP1", config);
		em = emf.createEntityManager();
	}

	public void initialize() throws Exception {
	};

	public void close() throws Exception {
		if (em != null) {
			/*
			 * À COMPLETER FERMER LA CONNEXION ET INITIALISER À NULL
			 */
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
			/*
			 * À COMPLÉTER : Créer des objets persistants pour Movie, People,
			 * Role
			 */
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

}
