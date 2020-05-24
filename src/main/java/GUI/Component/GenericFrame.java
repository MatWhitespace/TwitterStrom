package main.java.GUI.Component;

import main.java.FileHandler.FileManager;
import org.jfree.chart.JFreeChart;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.List;

public class GenericFrame extends JFrame implements GuiComponent{
    private JPanel panel, mainPanel;
    private FileManager fm;
    private HashMap<String, Set<String>> treemapData;
    private final List<Color> colors;

    public GenericFrame(final FileManager fm){
        this.fm = fm;
        this.treemapData = new HashMap<>();
        this.colors = makeColors();
    }


    private List<Color> makeColors() {
        List<Color> res = new ArrayList<>(8);
        res.add(Color.RED);
        res.add(Color.BLUE);
        res.add(Color.PINK);
        res.add(Color.ORANGE);
        res.add(Color.LIGHT_GRAY);
        res.add(Color.GREEN);
        res.add(Color.CYAN);
        return res;
    }

    public void init(){
        JPanel corePane = new JPanel();
        corePane.setLayout(new BorderLayout());

        this.mainPanel = new JPanel(new BorderLayout());

        mainPanel.add(makeTitlePane("NER Analysis"), BorderLayout.NORTH);

        mainPanel.add(makeTreeMapCharts(), BorderLayout.CENTER);

        corePane.add(mainPanel,BorderLayout.CENTER);

        add(new JScrollPane(corePane,
                JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                JScrollPane.HORIZONTAL_SCROLLBAR_NEVER));

        setPreferredSize(new Dimension(800,900));

        pack();
        setTitle("All Tweets Analysis");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);
        updateData();
    }

    private JPanel makeTreeMapCharts() {
        JPanel treeMapPanel = new JPanel(new GridLayout(0,2));

        int index = 0;
        for (String key : this.treemapData.keySet()){
            JPanel keyPanel = new JPanel(new BorderLayout());
            keyPanel.add(makeTitlePane(key.toUpperCase()), BorderLayout.NORTH);
            keyPanel.add(makeValuePanel(this.treemapData.get(key), index), BorderLayout.CENTER);
            treeMapPanel.add(keyPanel);
            index++;
        }

        return treeMapPanel;
    }

    private JPanel makeValuePanel(Set<String> values, int i) {
        JPanel centerValuePane = new JPanel(new GridLayout(4,3));
        centerValuePane.setBackground(colors.get(i%colors.size()));


        for (String value : values){
            JLabel label = new JLabel(value, SwingConstants.CENTER);
            label.setForeground(Color.WHITE);
            label.setFont(new Font(label.getFont().getFontName(), Font.PLAIN, 20));
            centerValuePane.add(label);
        }

        centerValuePane.setPreferredSize(new Dimension(350, 200));
        centerValuePane.setMaximumSize(new Dimension(400, 250));
        centerValuePane.setMinimumSize(new Dimension(300, 150));

        return centerValuePane;
    }

    private JPanel makeTitlePane(String title) {
        JPanel northPane = new JPanel();

        JLabel titleLab = new JLabel(title, SwingConstants.CENTER);
        if(title.equals("NER Analysis")) titleLab.setFont(new Font(JFreeChart.DEFAULT_TITLE_FONT.getFontName(), Font.BOLD, 35));
        else titleLab.setFont(JFreeChart.DEFAULT_TITLE_FONT);
        northPane.add(titleLab);

        northPane.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        northPane.setBackground(Color.white);
        return northPane;

    }

    @Override
    public void updateData() {
        BufferedReader br;
        try{
            br = fm.getRead();
            String line = br.readLine();
            while (line != null){
                String [] splitting = line.split("\t");
                Set<String> values = new TreeSet();
                for (int i=1; i<splitting.length && i<12 ;i++)
                    values.add(splitting[i]);
                treemapData.put(splitting[0],values);
                line = br.readLine();
            }
            fm.stopRead(br);

            BorderLayout bl = (BorderLayout) mainPanel.getLayout();
            mainPanel.remove(bl.getLayoutComponent((BorderLayout.CENTER)));
            mainPanel.add(makeTreeMapCharts(),BorderLayout.CENTER);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
