package name.herve.chess.pgnindexer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GentleThreadPoolExecutor implements ExecutorService {
	public static GentleThreadPoolExecutor newGentleThreadPoolExecutor(String name, int nbt) {
		return newGentleThreadPoolExecutor(name, nbt, nbt * 2, 250);
	}

	public static GentleThreadPoolExecutor newGentleThreadPoolExecutor(String name, int nbt, int max, long sleep) {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nbt, new NamedThreadFactory(name));
		return new GentleThreadPoolExecutor(executor, max, sleep);
	}

	private ThreadPoolExecutor executor;
	private int max;
	private long sleep;

	private GentleThreadPoolExecutor(ThreadPoolExecutor executor, int max, long sleep) {
		super();
		this.executor = executor;
		this.max = max;
		this.sleep = sleep;
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return executor.awaitTermination(timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		sleepIfNecessary();
		executor.execute(command);
	}

	public int getActiveCount() {
		return executor.getActiveCount();
	}

	public long getCompletedTaskCount() {
		return executor.getCompletedTaskCount();
	}

	public long getTaskCount() {
		return executor.getTaskCount();
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		sleepIfNecessary();
		return executor.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		sleepIfNecessary();
		return executor.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		sleepIfNecessary();
		return executor.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		sleepIfNecessary();
		return executor.invokeAny(tasks, timeout, unit);
	}

	@Override
	public boolean isShutdown() {
		return executor.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return executor.isTerminated();
	}

	@Override
	public void shutdown() {
		executor.shutdown();
	}

	public void shutdownAndAwaitTermination() {
		shutdown();
		try {
			awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.err.println("Interrupted in shutdownAndAwaitTermination()");
		}
		shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return executor.shutdownNow();
	}

	private void sleepIfNecessary() {
		while (!isShutdown() && ((executor.getActiveCount() + executor.getQueue().size()) >= max)) {
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				System.err.println("Interrupted in sleepIfNecessary()");
			}
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		sleepIfNecessary();
		return executor.submit(task);
	}

	@Override
	public Future<?> submit(Runnable task) {
		sleepIfNecessary();
		return executor.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		sleepIfNecessary();
		return executor.submit(task, result);
	}
}
