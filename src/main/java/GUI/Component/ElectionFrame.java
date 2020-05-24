package main.java.GUI.Component;


import main.java.FileHandler.FileManager;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ElectionFrame extends JFrame implements GuiComponent {
    private List<Double> datas;
    private JFreeChart histChart, xYChart;
    private boolean HIST;
    private FileManager fm;
    private final int MINUTES;

    public ElectionFrame(FileManager fm, final int MINUTES) {
        this.MINUTES = MINUTES;
        this.fm = fm;
        this.HIST = true;
        datas = new ArrayList<>(15);
        datas.add(50.0);
        datas.add(50.0);
    }

    public void init(){

        XYDataset xYDataset = createXYDataset();
        xYChart = createLine(xYDataset);

        CategoryDataset categoryDataset = createCategoryDataset();
        histChart = createHist(categoryDataset);

        ChartPanel chartPanel = new ChartPanel(histChart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.white);

        JPanel buttonPanel = new JPanel();
        JButton changeButton = new JButton("Cambia");
        changeButton.addActionListener((actionEvent -> {
            if (HIST) {
                chartPanel.setChart(xYChart);
                HIST = false;
            }else{
                chartPanel.setChart(histChart);
                HIST = true;
            }
        }));
        buttonPanel.add(changeButton);
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        buttonPanel.setBackground(Color.white);

        setLayout(new BorderLayout());
        add(chartPanel,BorderLayout.CENTER);
        add(buttonPanel, BorderLayout.SOUTH);
        pack();
        setTitle("Sondaggio");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);

    }

    public void updateData(){
        BufferedReader br;
        String[] line;
        try {
            br = fm.getRead();
            line = br.readLine().split("\\t");
            datas.add(Double.parseDouble(line[0].replaceAll(",",".")));
            datas.add(Double.parseDouble(line[1].replaceAll(",",".")));
            this.histChart.getCategoryPlot().setDataset(createCategoryDataset());
            this.xYChart.getXYPlot().setDataset(createXYDataset());
            fm.stopRead(br);
        }catch (Exception e ){}
        finally {
            if (datas.size()>20){
                datas.remove(0);
                datas.remove(1);
            }
        }

    }

    private CategoryDataset createCategoryDataset() {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.setValue(datas.get(datas.size()-2), "Percentuali", "Biden");
        dataset.setValue(datas.get(datas.size()-1), "Percentuali", "Trump");

        return dataset;
    }

    private JFreeChart createHist(CategoryDataset dataset) {

        JFreeChart barChart = ChartFactory.createBarChart(
                "Sondaggio Elezioni USA",
                "",
                "Percentuali",
                dataset,
                PlotOrientation.HORIZONTAL,
                false, true, false);

        return barChart;
    }

    private XYDataset createXYDataset() {

        XYSeries series1 = new XYSeries("Biden");
        XYSeries series2 = new XYSeries("Trump");

        int time = 0;
        for (int i =0; i<datas.size();i++){
            if (i%2==0) {
                time += MINUTES;
                series1.add(time, datas.get(i));
            }else
                series2.add(time,datas.get(i));
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series1);
        dataset.addSeries(series2);

        return dataset;
    }

    private JFreeChart createLine(final XYDataset dataset) {

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Andamento",
                "Tempo",
                "Percentuale",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        XYPlot plot = chart.getXYPlot();

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        renderer.setSeriesPaint(0, Color.RED);
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer.setSeriesPaint(1, Color.BLUE);
        renderer.setSeriesStroke(1, new BasicStroke(2.0f));

        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinesVisible(false);
        plot.setDomainGridlinesVisible(false);

        chart.getLegend().setFrame(BlockBorder.NONE);

        return chart;
    }
}
