package lcd.tp01;

/**
 *  Interface représentant l'accès au données (le modèle)
 * 
 */

interface IModel {

	/**
	 * Renvoie les noms de table sous forme de chaînes de caractères
	 */
  String [] getTableNames();
  
  /**
   * Ferme la connexion à la base de données
   * @throws Exception
   */
  void close () throws Exception;

  /**
   * Initialise les tables
   * @throws Exception
   */
  
  void initialize() throws Exception;

  /**
   * Prends en argument une Map associant des noms de tables à des fichiers
   * t -> f et remplit la table t avec le contenu du fichier f
   * @throws Exception
   */
  
  void fillTables(java.util.Map<String,java.io.File> files) throws Exception;
  
  /**
   * Interroge la base et renvoie la liste des résultats sous forme d'une collection de chaînes.
   * @param pattern : paramètre passé pour la requête
   * @return
   * @throws Exception
   */
  java.util.Collection<String> query(String pattern) throws Exception;
}
