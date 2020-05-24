package main.java.GUI.Component;

import main.java.FileHandler.FileManager;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Set;


public class CoronaFrame extends JFrame  implements GuiComponent{
    private HashMap<String,Integer>[] datas;
    private HashMap<Integer,String> mapping;
    private JFreeChart[][] charts;
    private boolean[] PIE;
    private FileManager[] fm;
    private final int MINUTES;
    private JTextPane textArea;


    private HashMap<Integer, String> createMapping(){
        HashMap<Integer,String> mapping = new HashMap<>();
        mapping.put(0,"Trends");
        mapping.put(1,"Verified Account Trends");
        mapping.put(2,"State Count Tweets");
        mapping.put(3,"Place Count Tweets");
        mapping.put(4,"Sentiment Analysis");
        mapping.put(5,"Popular Retweet List");
        return mapping;
    }

    /**
     * Ordinare i PieChart del count bolt per primi 4 seguendo l'ordine
     * del mapping, successivamente i posNeg e retweet
     *
     * @param fm
     * @param MINUTES
     */
    public CoronaFrame(final FileManager[] fm, final int MINUTES){
        this.mapping = createMapping();
        this.fm = fm;
        this.MINUTES = MINUTES;
        this.PIE = new boolean[4];
        this.PIE[0]=this.PIE[1]=this.PIE[2]=this.PIE[3]=true;
        this.charts = new JFreeChart[fm.length][2];
        this.datas = new HashMap[fm.length];
        for (int i =0; i<datas.length;i++)
            datas[i] = new HashMap<String,Integer>();
    }

    public void init() {

        JPanel pane = new JPanel();
        pane.setLayout(new GridLayout(0,2));

        DefaultPieDataset[] pieDatasets = new DefaultPieDataset[fm.length];
        DefaultCategoryDataset[] categoryDatasets = new DefaultCategoryDataset[fm.length];

        for (int i=0; i<5; i++) {

            JPanel centerPanel = new JPanel();
            centerPanel.setLayout(new BorderLayout());

            JFreeChart defaultChart = null;

            categoryDatasets[i] = createCategoryDataset(i);
            JFreeChart histChart = createHistChart(categoryDatasets[i], i);
            charts[i][1] = histChart;
            defaultChart = histChart;

            if(i<4) {
                pieDatasets[i] = createPieDataset(i);
                JFreeChart pieChart = createPieChart(pieDatasets[i], i);
                charts[i][0] = pieChart;
                defaultChart = pieChart;
            }

            ChartPanel chartPanel = new ChartPanel(defaultChart);
            chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
            chartPanel.setBackground(Color.white);

            centerPanel.add(chartPanel, BorderLayout.CENTER);

            if(i<4) {
                JPanel buttonPanel = new JPanel();

                JButton changeButton = new JButton("Cambia");

                int finalI = i;
                changeButton.addActionListener((actionEvent -> {
                    if (PIE[finalI]) {
                        chartPanel.setChart(charts[finalI][1]);
                        PIE[finalI] = false;
                    } else {
                        chartPanel.setChart(charts[finalI][0]);
                        PIE[finalI] = true;
                    }
                }));

                buttonPanel.add(changeButton);
                buttonPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
                buttonPanel.setBackground(Color.white);

                centerPanel.add(buttonPanel, BorderLayout.SOUTH);
            }
            centerPanel.setPreferredSize(new Dimension(400, 350));
            centerPanel.setMaximumSize(new Dimension(450, 350));
            centerPanel.setMinimumSize(new Dimension(350, 350));

            pane.add(centerPanel);
        }

        JPanel textPanel = new JPanel();
        textPanel.setLayout(new BorderLayout());

        JPanel titleTextPane = new JPanel();
        JLabel title = new JLabel(mapping.get(5));
        title.setFont(JFreeChart.DEFAULT_TITLE_FONT);
        titleTextPane.add(title);
        titleTextPane.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        titleTextPane.setBackground(Color.WHITE);

        textPanel.add(titleTextPane,BorderLayout.NORTH);

        textPanel.setPreferredSize(new Dimension(400, 350));
        textPanel.setMaximumSize(new Dimension(450, 350));
        textPanel.setMinimumSize(new Dimension(350, 350));

        this.textArea = makeRetweetsTextArea();
        JScrollPane scrollArea = new JScrollPane(
                this.textArea,
                JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                JScrollPane.HORIZONTAL_SCROLLBAR_NEVER
        );

        textPanel.add(scrollArea,BorderLayout.CENTER);

        pane.add(textPanel);


        add(new JScrollPane(pane,
                JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                JScrollPane.HORIZONTAL_SCROLLBAR_NEVER));

        pack();
        setTitle("Coronavirus Analysis");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);

        updateData();
    }

    @Override
    public void updateData(){
        BufferedReader br;
        HashMap<String, Integer> tmp;
        try {
            for (int i =0; i<this.fm.length;i++) {
                tmp = new HashMap<>();
                br = this.fm[i].getRead();
                String line = br.readLine();
                while (line != null) {
                    if (i<5) {
                        String[] splitting = line.split("\\t");
                        tmp.put(splitting[0], Integer.parseInt(splitting[1]));
                    }else{
                        tmp.put(line,-1);
                    }
                    line = br.readLine();
                }
                this.fm[i].stopRead(br);
                datas[i] = tmp;
                if(i <4) {
                    ((PiePlot) this.charts[i][0].getPlot()).setDataset(createPieDataset(i));
                    this.charts[i][1].getCategoryPlot().setDataset(createCategoryDataset(i));
                }else if (i==4){
                    this.charts[i][1].getCategoryPlot().setDataset(createCategoryDataset(i));
                }else{
                    updateText(this.textArea);
                }
            }
        }catch (Exception e ){
            e.printStackTrace();
        }

    }

    private DefaultCategoryDataset createCategoryDataset(int i) {
        HashMap<String,Integer> data = datas[i];

        String label = "Occurrences";
        if (i==4) label = "Percentuali";

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        for (String key : data.keySet()){
            dataset.setValue(data.get(key), label, key);
        }

        return dataset;

    }

    private JFreeChart createHistChart(DefaultCategoryDataset dataset, Integer i){
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        if (i==4) orientation = PlotOrientation.HORIZONTAL;

        String label = "Occurrences";
        if (i==4) label = "Percentuali";

        JFreeChart barChart = ChartFactory.createBarChart(
                mapping.get(i)+" Histogram",
                "",
                label,
                dataset,
                orientation,
                false, true, false);

        return barChart;

    }


    private JFreeChart createPieChart(DefaultPieDataset dataset, Integer i) {

        JFreeChart pieChart = ChartFactory.createPieChart(
                mapping.get(i)+" Pie Chart",
                dataset,
                true, true, false);

        pieChart.getLegend().setFrame(BlockBorder.NONE);

        return pieChart;

    }

    private DefaultPieDataset createPieDataset(int i) {

        HashMap<String,Integer> data = datas[i];
        DefaultPieDataset dataset = new DefaultPieDataset();

        for (String key : data.keySet()){
            dataset.setValue(key, data.get(key));
        }

        return dataset;
    }

    private JTextPane makeRetweetsTextArea(){
        JTextPane pane = new JTextPane();
        pane.setContentType("text/html");
        pane.setBorder(BorderFactory.createEmptyBorder(30, 30, 30, 30));

        updateText(pane);

        pane.setBackground(Color.WHITE);

        pane.setPreferredSize(new Dimension(400, 350));
        pane.setMaximumSize(new Dimension(450, 350));
        pane.setMinimumSize(new Dimension(350, 350));

        pane.setEditable(false);

        return pane;
    }
    private void updateText(JTextPane pane){
        Set<String> tweets = datas[5].keySet();
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"list\"><ul>");
        for (String tweet : tweets) {
            sb.append("<li><p>"+tweet+"</p></li><br>");
        }
        sb.append("</ul></div>");

        pane.setText(sb.toString());
    }

}
