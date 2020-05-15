package View;


import Control.Observers.Observer;
import Control.Subjects.StormSubject;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.util.LinkedList;
import java.util.List;

public class ElectionFrame extends JFrame implements Observer {
    private StormSubject subject;
    private List<Double> datas;
    private JFreeChart chart;

    public ElectionFrame(StormSubject stormSubject){
        this.subject = stormSubject;
        datas = new LinkedList<>();
        datas.add(50.0);
        datas.add(50.0);
    }

    private void init(){
        CategoryDataset dataset = createDataset();

        chart = createChart(dataset);
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.white);
        add(chartPanel);

        pack();
        setTitle("Bar chart");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);

    }

    @Override
    public void update() {
        datas.clear();
        for (String elem : subject.getState("Election")) {
            datas.add(Double.parseDouble(elem));
        }
        this.chart.getCategoryPlot().setDataset(createDataset());
    }

    private CategoryDataset createDataset() {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.setValue(datas.get(0), "Percentuali", "Biden");
        dataset.setValue(datas.get(0), "Percentuali", "Trump");

        return dataset;
    }

    private JFreeChart createChart(CategoryDataset dataset) {

        JFreeChart barChart = ChartFactory.createBarChart(
                "Sondaggio Elezioni USA",
                "",
                "Percentuali",
                dataset,
                PlotOrientation.HORIZONTAL,
                false, true, false);

        return barChart;
    }
}
