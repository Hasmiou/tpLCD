package lcd.tp01;

import java.io.File;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;

import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.ListView;
import javafx.scene.layout.Pane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.FileChooser;
import javafx.stage.WindowEvent;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;

//On étend la classe application:

public class View extends Application {

	IModel dbm;
	Map<String, File> files;
	File previousDir;

	public static void main(String[] args) {
		launch(args); // Défini dans la classe Application
	}

	// Méthode appelée au démarrage de l'application, prend en argument
	// un "Stage" qui est une abstraction pour la fenêtre

	@Override
	public void start(Stage primaryStage) {
		try {
			dbm = new JDBCModel("knguye10_a", "knguye10_a", "knguye10_a");
			//dbm = new JDBCModel("knguye10_a", "knguye10_a", "knguye10_a");
			files = new TreeMap<String, File>();
			previousDir = null;
			primaryStage.setTitle("Movie DB");

			Pane root = new VBox(1 + dbm.getTableNames().length);
			root.setPadding(new Insets(5.0));
			primaryStage.setScene(new Scene(root));

			primaryStage.setOnCloseRequest(new EventHandler<WindowEvent>() {

				@Override
				public void handle(WindowEvent event) {
					try {
						dbm.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			});

			Button btnCreate = new Button();
			btnCreate.setText("Create Tables");
			btnCreate.setOnAction(new EventHandler<ActionEvent>() {

				@Override
				public void handle(ActionEvent event) {

					try {
						dbm.initialize();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("Table created");
				}

			});

			root.getChildren().add(btnCreate);

			for (String s : dbm.getTableNames()) {
				Button btnLoad = new Button();
				Label lab = new Label();
				HBox hbox = new HBox(2);
				hbox.getChildren().addAll(btnLoad, lab);
				btnLoad.setText(s + " data file");
				btnLoad.setOnAction(new EventHandler<ActionEvent>() {
					@Override
					public void handle(ActionEvent event) {
						FileChooser fileChooser = new FileChooser();
						fileChooser.setTitle("Data file for table " + s);
						if (previousDir != null)
							fileChooser.setInitialDirectory(previousDir);
						File f = fileChooser.showOpenDialog(primaryStage);
						if (f != null) {
							files.put(s, f);
							lab.setText(f.getAbsolutePath());
							previousDir = f.getParentFile();
						}
					}
				});
				root.getChildren().add(hbox);

			}
			;

			Button btnLoad = new Button();
			btnLoad.setText("Populate tables");
			btnLoad.setOnAction(new EventHandler<ActionEvent>() {
				@Override
				public void handle(ActionEvent event) {

					try {
						dbm.fillTables(files);
						System.out.println("Tables populated");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			});
			root.getChildren().add(btnLoad);
			HBox hbx = new HBox(2);
			root.getChildren().add(hbx);
			hbx.getChildren().add(new Label("Search:"));
			TextField txt = new TextField();

			txt.setPrefColumnCount(40);
			hbx.getChildren().add(txt);


			ListView<String> list = new ListView<String>();
			root.getChildren().add(list);

			Button btnQuery = new Button();
			btnQuery.setText("Search !");
			btnQuery.setOnAction(new EventHandler<ActionEvent>() {
				@Override
				public void handle(ActionEvent event) {

					try {
						if (txt.getText().trim().isEmpty())
							return;

						Collection<String> results = dbm.query(txt.getText().trim());
						list.getItems().clear();
						list.getItems().addAll(results);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			});

			root.getChildren().add(btnQuery);

			primaryStage.show();

		} catch (Exception e) {
			if (dbm != null) try { dbm.close();} catch (Exception _e) {};
			e.printStackTrace();
		}

	}

}
