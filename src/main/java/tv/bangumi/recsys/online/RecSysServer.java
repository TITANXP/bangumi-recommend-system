package tv.bangumi.recsys.online;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import tv.bangumi.recsys.online.datamanager.DataManager;
import tv.bangumi.recsys.online.service.*;

import java.net.InetSocketAddress;


public class RecSysServer {
    private static Logger logger = Logger.getLogger(RecSysServer.class);

    private static final int DEFAULT_PORT = 6010;
    public static void main(String[] args) throws Exception {
        new RecSysServer().run();
    }

    public void run() throws Exception {
        logger.info("Server has started");

        InetSocketAddress inetAddress = new InetSocketAddress("localhost", DEFAULT_PORT);
        Server server = new Server(inetAddress);
        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(new ServletHolder(new AnimeService()), "/getAnime");
        context.addServlet(new ServletHolder(new UserService()), "/getUser");
        context.addServlet(new ServletHolder(new RecForYouService()), "/getRec");
        context.addServlet(new ServletHolder(new SimilarAnimeService()), "/getSimAnime");
        context.addServlet(new ServletHolder(new SimilarUserService()), "/getSimUser");
        context.addServlet(new ServletHolder(new AnimeSearchService()), "/searchAnime");
        context.addServlet(new ServletHolder(new SearchSuggestService()), "/searchSuggest");


        // 加载user和item的embedding向量
        DataManager dataManager = DataManager.getInstance();
//        dataManager.loadUserEmb("userEmb.csv"); 使用redis，不从文件加载
//        dataManager.loadItemEmb("itemEmbWord2vec.csv");
        dataManager.loadAnimeId();

        server.setHandler(context);
        logger.info("Server has started");

        server.start();
        server.join();
    }
}

// docker run -t --rm -p 8501:8501 -v "D:\IDEA\bangumi-recommend-system\src\main\resources\model\neuralcf:/models/recmodel" -e MODEL_NAME=recmodel tensorflow/serving
