package name.herve.chess.pgnindexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.github.bhlangonijr.chesslib.Board;
import com.github.bhlangonijr.chesslib.game.Game;
import com.github.bhlangonijr.chesslib.game.Player;
import com.github.bhlangonijr.chesslib.move.Move;
import com.github.bhlangonijr.chesslib.move.MoveList;
import com.github.bhlangonijr.chesslib.pgn.PgnException;
import com.github.bhlangonijr.chesslib.pgn.PgnIterator;

public class PGNIndexer {
	private class FileTask implements Callable<FileTask> {
		private File file;

		private Path root;

		public FileTask(File file, Path root) {
			super();
			this.file = file;
			this.root = root;
		}

		@Override
		public FileTask call() throws Exception {
			// log(file.getName() + " ...");
			PgnIterator games = null;

			try {
				games = new PgnIterator(file.getAbsolutePath());
			} catch (Exception e) {
				// log(true, e);
			}
			int fileCounter = 0;
			String fileName = root.relativize(file.toPath()).toString();

			try {
				for (Game game : games) {
					fileCounter++;
					game.setGameId("[" + fileCounter + "]" + fileName);
					indexGame(game);
				}
			} catch (Exception e) {
				// log(true, e);
			}

			synchronized (PGNIndexer.this) {
				nbIndexedFiles++;
			}
			// log("... " + file.getName());
			return this;
		}
	}

	private class GameTask implements Callable<GameTask> {
		private Game game;
		private Set<Long> positions;

		public GameTask(Game game) {
			super();
			this.game = game;
		}

		@Override
		public GameTask call() throws Exception {
			MoveList moves = game.getHalfMoves();
			Board board = new Board();
			positions = new TreeSet<>();
			try {
				for (Move move : moves) {
					board.doMove(move);
					nbIndexedMoves++;
					long zobrist = board.getZobristKey();
					positions.add(zobrist);
					// String fen = board.getFen(false);
					// fen = fen.substring(0, fen.indexOf(" "));
					// String otherFen;
					// synchronized (indexedPositions) {
					// otherFen = indexedPositions.put(zobrist, fen);
					// }
					// if ((otherFen != null) && !otherFen.equals(fen)) {
					// log(true, "Zobrist hash collision " + zobrist);
					// log(true, " " + fen);
					// log(true, " " + otherFen);
					// }
				}

				if (dbFileChannel != null) {
					long[] pos = positions.stream().mapToLong(Long::longValue).toArray();
					synchronized (dbFileChannel) {
						PersistenceToolbox.dumpString(dbFileChannel, game.getGameId());
						PersistenceToolbox.dumpFullLongArray(dbFileChannel, pos);
					}
				}
			} catch (NullPointerException e) {
				// e.printStackTrace();
			}
			// log("Processed : " + gameToShortString(game));
			return this;
		}
	}

	private static DecimalFormat DECF = new DecimalFormat("###,###");

	private static SimpleDateFormat DTF;

	static {
		DTF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		DTF.setTimeZone(TimeZone.getTimeZone("Europe/Paris"));
	}

	private static void log(boolean isError, String msg) {
		@SuppressWarnings("resource")
		PrintStream ps = isError ? System.err : System.out;
		ps.println("[" + DTF.format(new Date()) + "]   " + msg);
	}

	private static void log(boolean isError, Throwable e) {
		if ((e instanceof PgnException) && (e.getCause() != null)) {
			log(isError, e.getMessage() + " :: " + e.getCause().getMessage());
		} else {
			log(isError, e.getClass().getName() + " : " + e.getMessage());
		}
	}

	private static void log(String msg) {
		log(false, msg);
	}

	public static void main(String[] args) {
		PGNIndexer indexer = new PGNIndexer();
		indexer.start(args);
	}

	private long nbIndexedGames;
	private long nbIndexedMoves;
	private long nbIndexedFiles;
	private GentleThreadPoolExecutor fileExecutor;
	private GentleThreadPoolExecutor gameExecutor;
	private List<Future<FileTask>> fileTasks;
	private FileChannel dbFileChannel;
	private FileChannel dbIndexChannel;

	private Map<Integer, String> indexedGames = null;
	private Map<Long, int[]> index = null;

	public PGNIndexer() {
		super();
		nbIndexedGames = 0;
		nbIndexedMoves = 0;
		nbIndexedFiles = 0;
	}

	private String gameToShortString(Game game) {
		StringBuilder sb = new StringBuilder();

		sb.append(game.getGameId());
		if (game.getVariant() != null) {
			sb.append(" {");
			sb.append(game.getVariant());
			sb.append("}");
		}
		sb.append(" : ");
		sb.append(game.getDate());
		sb.append(" ");
		sb.append(playerToShortString(game.getWhitePlayer()));
		sb.append(" ");
		if (game.getResult() != null) {
			sb.append(game.getResult().getDescription());
		} else {
			sb.append("{no result parsed}");
		}
		sb.append(" ");
		sb.append(playerToShortString(game.getBlackPlayer()));

		return sb.toString();
	}

	private void indexFile(File file, Path root, boolean recursive) throws IOException {
		// log("indexFile : " + fileTasks.size() + " - " + file);
		if (!file.exists()) {
			log(true, "Unknown file " + file);
			return;
		}

		if (file.isDirectory()) {
			List<File> listing = Arrays.asList(file.listFiles());
			Collections.sort(listing);
			for (File s : listing) {
				if (s.isFile() || (s.isDirectory() && recursive)) {
					indexFile(s, root, recursive);
				}
			}
			return;
		}

		fileTasks.add(fileExecutor.submit(new FileTask(file, root)));
		log("file tasks : " + fileTasks.size() + " - " + file);
	}

	private Future<GameTask> indexGame(Game game) {
		nbIndexedGames++;

		if (!Game.isParsable(game) || (game.getExceptionInParsing() != null)) {
			// log("Chesslib had an error while parsing : " + gameToShortString(game));
			// log(false, game.getExceptionInParsing());
			return null;
		}

		return gameExecutor.submit(new GameTask(game));
	}

	private String playerToShortString(Player player) {
		StringBuilder sb = new StringBuilder();
		sb.append(player.getName());
		sb.append("[");
		sb.append(player.getElo());
		sb.append("]");
		return sb.toString();
	}

	private int queryBoard(Board b) {
		long zobrist = b.getZobristKey();
		int[] pos = index.get(zobrist);
		if (pos != null) {
			return pos.length;
		}
		return 0;
	}

	private int queryFen(String fen) {
		try {
			Board board = new Board();
			board.loadFromFen(fen);
			return queryBoard(board);
		} catch (Exception e) {
			log(true, e);
			return 0;
		}
	}

	private void queryPgn(String pgn) {
		try {
			PgnIterator games = new PgnIterator(new File(pgn).getAbsolutePath());
			int fileCounter = 0;
			for (Game game : games) {
				fileCounter++;
				game.setGameId("game " + DECF.format(fileCounter));
				System.out.println(gameToShortString(game));
				MoveList moves = game.getHalfMoves();
				Board board = new Board();
				for (Move move : moves) {
					board.doMove(move);
					int n = queryBoard(board);
					System.out.println("      . " + DECF.format(board.getMoveCounter()) + " - " + DECF.format(n) + " / " + DECF.format(indexedGames.size()));
				}
			}
		} catch (Exception e) {
			log(true, e);
		}
	}

	private void query() {
		boolean shouldExit = false;
		Scanner kb = new Scanner(System.in);

		while (!shouldExit) {
			System.out.print("> ");
			String q = kb.nextLine();

			q = q.strip();

			if ("exit".equalsIgnoreCase(q)) {
				shouldExit = true;
			} else if (q.toLowerCase().startsWith("load")) {
				q = q.substring(4).strip();
				queryPgn(q);
			} else {
				int n = queryFen(q);
				System.out.println("found in " + DECF.format(n) + " / " + DECF.format(indexedGames.size()) + " games");
			}
		}

		kb.close();
	}

	public void start(String[] args) {
		Options options = new Options();
		options.addOption("p", "pgn", true, "PGN file to load");
		options.addOption("d", "database", true, "database file");
		options.addOption("i", "index", true, "index file");

		CommandLineParser parser = new DefaultParser();
		CommandLine command = null;
		try {
			command = parser.parse(options, args);
		} catch (ParseException e) {
			log(true, e);
			return;
		}

		File pgnFile = command.hasOption('p') ? new File(command.getOptionValue('p')) : null;
		File dbFile = command.hasOption('d') ? new File(command.getOptionValue('d')) : null;
		File idxFile = command.hasOption('i') ? new File(command.getOptionValue('i')) : null;

		boolean createDb = (pgnFile != null) && (dbFile != null);
		boolean loadDb = (pgnFile == null) && (dbFile != null);
		boolean createIdx = (dbFile != null) && (idxFile != null);
		boolean loadIdx = (dbFile == null) && (idxFile != null);

		if (createDb) {
			try {
				fileExecutor = GentleThreadPoolExecutor.newGentleThreadPoolExecutor("file", Runtime.getRuntime().availableProcessors(),
						Runtime.getRuntime().availableProcessors() * 3, 250);
				gameExecutor = GentleThreadPoolExecutor.newGentleThreadPoolExecutor("game", Runtime.getRuntime().availableProcessors(), 10000, 250);
				fileTasks = Collections.synchronizedList(new ArrayList<>());

				try {
					RandomAccessFile raf = PersistenceToolbox.getFile(dbFile, true);
					dbFileChannel = raf.getChannel();
				} catch (FileNotFoundException e) {
					log(true, e);
					return;
				}

				indexFile(pgnFile, pgnFile.toPath(), true);
			} catch (Exception e) {
				log(true, e);
			} finally {
				log("Shutting down the thread pool 1/2");
				for (Future<FileTask> t : fileTasks) {
					try {
						t.get();
					} catch (InterruptedException | ExecutionException e) {
						log(true, e);
					}
				}

				fileExecutor.shutdown();
				try {
					fileExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
				}
				fileExecutor.shutdownNow();

				log("Shutting down the thread pool 2/2");
				gameExecutor.shutdown();
				try {
					gameExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
				}
				gameExecutor.shutdownNow();

				stats();

				if (dbFileChannel != null) {
					try {
						dbFileChannel.close();
						log(dbFile + " closed");
					} catch (IOException e) {
					}
				}
			}
		}

		Map<Long, List<Integer>> indexedPositions = null;
		int gameIndex = 0;
		if (loadDb) {
			try {
				log("Loading " + dbFile);
				indexedGames = new HashMap<>();
				indexedPositions = new HashMap<>();
				gameIndex = 0;

				try {
					RandomAccessFile raf = PersistenceToolbox.getFile(dbFile, false);
					dbFileChannel = raf.getChannel();
				} catch (FileNotFoundException e) {
					log(true, e);
					return;
				}

				while (true) {
					String gameId = PersistenceToolbox.loadString(dbFileChannel);
					long[] pos = PersistenceToolbox.loadFullLongArray(dbFileChannel);
					gameIndex++;
					indexedGames.put(gameIndex, gameId);
					for (long p : pos) {
						List<Integer> games = indexedPositions.get(p);
						if (games == null) {
							games = new ArrayList<>();
							indexedPositions.put(p, games);
						}
						games.add(gameIndex);
					}
				}
			} catch (BufferUnderflowException e) {
				// eof
			} catch (IOException e) {
				log(true, e);
			} finally {
				if (dbFileChannel != null) {
					try {
						dbFileChannel.close();
						log(dbFile + " closed");
					} catch (IOException e) {
					}
				}
			}
		}

		if (createIdx) {
			try {
				RandomAccessFile raf = PersistenceToolbox.getFile(idxFile, true);
				dbIndexChannel = raf.getChannel();

				PersistenceToolbox.dumpInt(dbIndexChannel, indexedGames.size());
				for (Entry<Integer, String> e : indexedGames.entrySet()) {
					PersistenceToolbox.dumpInt(dbIndexChannel, e.getKey());
					PersistenceToolbox.dumpString(dbIndexChannel, e.getValue());
				}

				PersistenceToolbox.dumpInt(dbIndexChannel, indexedPositions.size());
				for (Entry<Long, List<Integer>> e : indexedPositions.entrySet()) {
					PersistenceToolbox.dumpLong(dbIndexChannel, e.getKey());
					int[] pos = e.getValue().stream().mapToInt(Integer::intValue).toArray();
					PersistenceToolbox.dumpFullIntArray(dbIndexChannel, pos);
				}
			} catch (IOException e) {
				log(true, e);
				return;
			} finally {
				if (dbIndexChannel != null) {
					try {
						dbIndexChannel.close();
						log(idxFile + " closed");
					} catch (IOException e) {
					}
				}
			}
		}

		if (loadIdx) {
			try {
				log("Loading " + idxFile);
				indexedGames = new HashMap<>();
				index = new HashMap<>();

				try {
					RandomAccessFile raf = PersistenceToolbox.getFile(idxFile, false);
					dbIndexChannel = raf.getChannel();
				} catch (FileNotFoundException e) {
					log(true, e);
					return;
				}

				int nbGames = PersistenceToolbox.loadInt(dbIndexChannel);
				log(". nbGames " + DECF.format(nbGames));
				for (int g = 0; g < nbGames; g++) {
					int id = PersistenceToolbox.loadInt(dbIndexChannel);
					String gameFile = PersistenceToolbox.loadString(dbIndexChannel);
					indexedGames.put(id, gameFile);
				}

				int nbPos = PersistenceToolbox.loadInt(dbIndexChannel);
				log(". nbPos " + DECF.format(nbPos));
				int pct10 = nbPos / 10;
				for (int p = 0; p < nbPos; p++) {
					long pos = PersistenceToolbox.loadLong(dbIndexChannel);
					int[] files = PersistenceToolbox.loadFullIntArray(dbIndexChannel);
					index.put(pos, files);
					if ((p % pct10) == 0) {
						log("    " + DECF.format(p));
					}
				}

			} catch (IOException e) {
				log(true, e);
				return;
			} finally {
				if (dbIndexChannel != null) {
					try {
						dbIndexChannel.close();
						log(idxFile + " closed");
					} catch (IOException e) {
					}
				}
			}
		}

		query();

	}

	private void stats() {
		log(DECF.format(nbIndexedFiles) + " files indexed, " + DECF.format(nbIndexedGames) + " games indexed, " + DECF.format(nbIndexedMoves) + " moves indexed");
	}
}
