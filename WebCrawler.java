package info.kgeorgiy.ja.muminova.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Crawls web sites.
 *
 * @author Sobira Muminova
 */

public class WebCrawler implements Crawler {
    private final Downloader downloader;
    private final ExecutorService downloaders;
    private final ExecutorService extractors;
    private final int perHostLim;
    private final Map<String, HostWorker> hostMap = new ConcurrentHashMap<>();
    private final Phaser phaser = new Phaser(1);

    /**
     * Initializes variables
     *
     * @param downloader downloads page by url
     * @param downloaders max size of downloading pages
     * @param extractors max size of reading pages
     * @param perHost max size of pages downloading fro current host
     */
    public WebCrawler(Downloader downloader, int downloaders, int extractors, int perHost) {
        this.downloader = downloader;
        this.downloaders = Executors.newFixedThreadPool(downloaders);
        this.extractors = Executors.newFixedThreadPool(extractors);
        this.perHostLim = perHost;
    }

    /**
     * Handles arguments and run download method
     *ja
     * @param args list of specified arguments
     */
    public static void main(String[] args) {
        if (args.length == 0 || Arrays.stream(args).anyMatch(Objects::isNull) || args.length > 5) {
            System.err.println("Wrong number of arguments. Try this: WebCrawler url [depth [downloads [extractors [perHost]]]]");
            return;
        }
        int[] arguments = new int[]{2, 64, 8, 16};
        try {
            for (int i = 1; i < args.length; i++) {
                arguments[i - 1] = Integer.parseInt(args[i]);
            }
            WebCrawler crawler = new WebCrawler(new CachingDownloader(), arguments[1], arguments[2], arguments[3]);
            Result res = crawler.download(args[0], arguments[0]);
            System.out.println(res.getDownloaded());
        } catch (NumberFormatException e) {
            System.err.println("Expected number but found string. Try this: WebCrawler url [depth [downloads [extractors [perHost]]]]");
        } catch (IOException e) {
            System.err.println("An error occurred : " + e.getMessage());
        }
    }

    /**
     * Downloads web site up to specified depth.
     *
     * @param url start <a href="http://tools.ietf.org/html/rfc3986">URL</a>.
     * @param depth download depth.
     * @return download result
     */
    @Override
    public Result download(String url, int depth) {
        Map<String, IOException> errors = new ConcurrentHashMap<>();
        Set<String> downloads = ConcurrentHashMap.newKeySet();
        Set<String> passed = ConcurrentHashMap.newKeySet();

        Queue<String> queue = new ConcurrentLinkedQueue<>(Set.of(url));

        passed.add(url);
        for (int counter = 0; !queue.isEmpty(); --counter) {
            String curUrl = queue.poll();
            if (downloads.contains(curUrl)) {
                continue;
            }

            processUrl(depth, downloads, errors, passed, queue, curUrl);

            if (counter == 0) {
                phaser.arriveAndAwaitAdvance();
                counter = queue.size();
            }
        }
        return new Result(new ArrayList<>(downloads), errors);
    }

    /**
     * Closes this web-crawler, relinquishing any allocated resources.
     */
    @Override
    public void close() {
        downloaders.shutdown();
        extractors.shutdown();
        try {
            if (!extractors.awaitTermination(500, TimeUnit.SECONDS) || !downloaders.awaitTermination(500, TimeUnit.SECONDS)) {
                System.err.println("Could not close ExecutorService");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while closing executors. " + e.getMessage());
        }
    }

    private void processUrl(int depth, Set<String> downloads, Map<String, IOException> errors,
                            Set<String> passed, Queue<String> queue, String url) {
        try {
            String host = URLUtils.getHost(url);
            phaser.register();

            final HostWorker hostQueue = hostMap.computeIfAbsent(host, x -> new HostWorker(downloaders));

            hostQueue.add(() -> {
                try {
                    final Document curDoc = downloader.download(url);
                    downloads.add(url);

                    if (phaser.getPhase() + 1 < depth) {
                        phaser.register();

                        extractors.submit(() -> {
                            try {
                                curDoc.extractLinks().forEach(x -> {
                                    if (!passed.contains(x) && !queue.contains(x)) {
                                        queue.add(x);
                                        passed.add(x);
                                    }
                                });
                            } catch (final IOException e) {
                                errors.put(url, e);
                            } finally {
                                phaser.arriveAndDeregister();
                            }
                        });
                    }
                } catch (final IOException e) {
                    errors.put(url, e);
                } finally {
                    phaser.arriveAndDeregister();
                    hostQueue.run();
                }
            });
        } catch (final MalformedURLException e) {
            errors.put(url, e);
        }
    }

    private class HostWorker {
        private final ExecutorService downloadService;
        private final Queue<Runnable> waiting = new ArrayDeque<>();
        private int running = 0;

        public HostWorker(final ExecutorService downloadService) {
            this.downloadService = downloadService;
        }

        public synchronized void add(final Runnable runnable) {
            if (running >= perHostLim) {
                waiting.add(runnable);
            } else {
                downloadService.submit(runnable);
                ++running;
            }
        }

        public synchronized void run() {
            final Runnable task = waiting.poll();
            if (Objects.nonNull(task)) {
                downloadService.submit(task);
            } else {
                --running;
            }
        }
    }
}