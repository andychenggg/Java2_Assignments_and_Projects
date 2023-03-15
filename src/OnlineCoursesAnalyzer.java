

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a class to read and integrate the data.
 */
public class OnlineCoursesAnalyzer {

    private final List<Course> courses = new ArrayList<>();

    private final Supplier<Stream<Course>> streamSupplier = courses::stream;

    /**
     * Constructor retrieve the data from the datasetPath and use a List to store the data.
     *
     * @param datasetPath the path of the dataset
     */
    public OnlineCoursesAnalyzer(String datasetPath) {
        try (BufferedReader bfr = new BufferedReader(
            new FileReader(datasetPath, StandardCharsets.UTF_8))) {
            // skip the first line
            String line = bfr.readLine();
            while ((line = bfr.readLine()) != null) {
                String[] result = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                for (int i = 0; i < result.length; i++) {
                    if (result[i].startsWith("\"")) {
                        result[i] = result[i].substring(1);
                    }
                    if (result[i].endsWith("\"")) {
                        result[i] = result[i].substring(0, result[i].length() - 1);
                    }
                }
                courses.add(new Course(result[0], result[1],
                    LocalDate.parse(result[2], DateTimeFormatter.ofPattern("MM/dd/yyyy")),
                    result[3], result[4], result[5], Integer.parseInt(result[6]),
                    Integer.parseInt(result[7]), Integer.parseInt(result[8]),
                    Integer.parseInt(result[9]), Integer.parseInt(result[10]),
                    Double.parseDouble(result[11]), Double.parseDouble(result[12]),
                    Double.parseDouble(result[13]), Double.parseDouble(result[14]),
                    Double.parseDouble(result[15]), Double.parseDouble(result[16]),
                    Double.parseDouble(result[17]), Double.parseDouble(result[18]),
                    Double.parseDouble(result[19]), Double.parseDouble(result[20]),
                    Double.parseDouble(result[21]), Double.parseDouble(result[22])));
                // System.out.println(result.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get participants count by instructors.
     *
     * @return Map&lt;String, Integer&rt; a relationship between instructors
     *          and participants of all his courses
     */
    public Map<String, Integer> getPtcpCountByInst() {

        Map<String, Integer> rawMap = streamSupplier.get().collect(
            Collectors.groupingBy(Course::getInstitution,
                Collectors.summingInt(Course::getParticipants)));
        // use TreeMap to initiate a treeMap, which sorted by natural order
        return new TreeMap<>(rawMap);
    }

    /**
     * Get participants count by instructor and subjects.
     *
     * @return Map&lt;String, Integer&rt; a relationship between instructor-subject
     *          and the corresponding participants
     */
    public Map<String, Integer> getPtcpCountByInstAndSubject() {
        // distinct key is guaranteed here
        Map<String, Integer> rawMap = streamSupplier.get().collect(
            Collectors.groupingBy(c -> c.getInstitution() + "-" + c.getCourseSubject(),
                Collectors.summingInt(Course::getParticipants)));
        Map<String, Integer> result = new TreeMap<>((s1, s2) -> (Objects.equals(rawMap.get(s1),
            rawMap.get(s2))) ? s2.compareTo(s1)
            : rawMap.get(s2) - rawMap.get(s1)); // descending order
        result.putAll(rawMap);
        return result;
    }

    /**
     * Get courses list of instructors.
     *
     * @return Map&lt;String, List&lt;List&lt;String&rt;&rt;&rt; a relationship between instructor
     *          and a List consist of a sublist of individually taught courses and
     *          a sublist of Collaboratively taught courses
     */
    public Map<String, List<List<String>>> getCourseListOfInstructor() {
        Map<String, List<Course>> instructorCourse = streamSupplier.get()
            .collect(Collectors.groupingBy(Course::getInstructors));
        Map<String, List<List<String>>> result = new HashMap<>();
        instructorCourse.forEach((s, l) -> {
            // no ","
            if (!s.contains(",")) {
                // key doesn't exist
                if (!result.containsKey(s)) {
                    List<List<String>> lls = new ArrayList<>();
                    lls.add(new ArrayList<>());
                    lls.add(new ArrayList<>());
                    result.put(s, lls);
                }
                // add the course title
                for (Course c : l) {
                    result.get(s).get(0).add(c.getCourseTitle());
                }
            } else { // have ","
                String[] profs = s.split(", ");
                for (String p : profs) {
                    // key doesn't exist
                    if (!result.containsKey(p)) {
                        List<List<String>> lls = new ArrayList<>();
                        lls.add(new ArrayList<>());
                        lls.add(new ArrayList<>());
                        result.put(p, lls);
                    }
                    // add the course title
                    for (Course c : l) {
                        result.get(p).get(1).add(c.getCourseTitle());
                    }
                }
            }
        });
        result.forEach((s, lls) -> {
            lls.get(0).sort(Comparator.naturalOrder());
            List<String> l = lls.get(0).stream().distinct().toList();
            lls.remove(0);
            lls.add(0, l);
            lls.get(1).sort(Comparator.naturalOrder());
            l = lls.get(1).stream().distinct().toList();
            lls.remove(1);
            lls.add(1, l);
        });
        return result;
    }

    /**
     * Get topK courses by a criteria.
     *
     * @param topK The number of the most top courses you want to select
     * @param by The criteria you select, either hours or participants
     *
     * @return return a list of the courses name
     */
    public List<String> getCourses(int topK, String by) {
        if (by.equals("hours")) {
            // descending
            return streamSupplier.get().sorted((c1, c2) -> {
                if (c1.getTotalCourseHours() - c2.getTotalCourseHours() > 0) {
                    return -1;
                } else if (c1.getTotalCourseHours() == c2.getTotalCourseHours()) {
                    return c1.getCourseTitle().compareTo(c2.getCourseTitle());
                } else {
                    return 1;
                }
            }).map(Course::getCourseTitle).distinct().limit(topK).toList();
        } else {
            // descending
            return streamSupplier.get().sorted((c1, c2) -> {
                if (c1.getParticipants() - c2.getParticipants() > 0) {
                    return -1;
                } else if (c1.getParticipants() == c2.getParticipants()) {
                    return c1.getCourseTitle().compareTo(c2.getCourseTitle());
                } else {
                    return 1;
                }
            }).map(Course::getCourseTitle).distinct().limit(topK).toList();
        }
    }

    /**
     * Search courses by some restrictions.
     *
     * @param courseSubject the subject you're interested in, the search is case-insensitive
     * @param percentAudited set a lower bound of the audited percent
     * @param totalCourseHours set an upper bound of the total course hours
     *
     * @return a list contains the titles of the appropriate courses
     */
    public List<String> searchCourses(String courseSubject, double percentAudited,
        double totalCourseHours) {
        return streamSupplier.get().filter(course -> Pattern
                .compile(".*" + courseSubject + ".*", Pattern.CASE_INSENSITIVE)
                .matcher(course.getCourseSubject()).matches())
            .filter(c -> c.getAuditedPercent() >= percentAudited)
            .filter(c -> c.getTotalCourseHours() <= totalCourseHours)
            .map(Course::getCourseTitle)
            .distinct()
            .sorted(Comparator.naturalOrder())
            .toList();
    }

    /**
     * Give a list of recommended courses according to the age, gender, and whether it's a bachelor.
     *
     * @param age the age of a person
     * @param gender 1 for male, 0 for female
     * @param isBachelorOrHigher 1 means "is bachelor", 0 means "is not a bachelor"
     *
     * @return return a list contains the titles of 10 most recommended courses
     */
    public List<String> recommendCourses(int age, int gender, int isBachelorOrHigher) {
        // groupingBy course number
        Map<String, Double> medianAges = streamSupplier.get().collect(
            Collectors.groupingBy(Course::getCourseNumber,
                Collectors.averagingDouble(Course::getMedianAge)));
        Map<String, Double> malePercents = streamSupplier.get().collect(
            Collectors.groupingBy(Course::getCourseNumber,
                Collectors.averagingDouble(Course::getMalePercent)));
        Map<String, Double> bachelorOrHigherPercents = streamSupplier.get().collect(
            Collectors.groupingBy(Course::getCourseNumber,
                Collectors.averagingDouble(Course::getBachelorDegreeOrHigher)));
        Map<String, List<Course>> courses = streamSupplier.get()
            .collect(Collectors.groupingBy(Course::getCourseNumber));
        // Get the latest courses
        Map<String, Course> latestCourses = new HashMap<>();
        courses.forEach((s, lls) ->
            latestCourses.put(s, lls.stream().max((c1, c2) -> {
                if (c1.getLaunchDate().isBefore(c2.getLaunchDate())) {
                    return -1;
                } else if (c1.getLaunchDate().isEqual(c2.getLaunchDate())) {
                    return 0;
                } else {
                    return 1;
                }
            }).get())
        );
        // get the courseNumber list
        List<String> courseNumber = streamSupplier.get().map(Course::getCourseNumber).distinct()
            .toList();
        Map<String, Double> similarity = new HashMap<>();
        courseNumber.forEach(num -> {
            double similarityValue = (age - medianAges.get(num)) * (age - medianAges.get(num))
                + (gender * 100 - malePercents.get(num)) * (gender * 100 - malePercents.get(num))
                + (isBachelorOrHigher * 100 - bachelorOrHigherPercents.get(num)) * (
                isBachelorOrHigher * 100 - bachelorOrHigherPercents.get(num));
            similarity.put(num, similarityValue);
        });
        return similarity.entrySet().stream().sorted((e1, e2) ->
                (e1.getValue() - e2.getValue() > 0) ? 1 :
                    (e1.getValue() - e2.getValue() == 0.0) ? latestCourses.get(e1.getKey())
                        .getCourseTitle().compareTo(latestCourses.get(e2.getKey()).getCourseTitle())
                        : -1)
            .map(e -> latestCourses.get(e.getKey()).getCourseTitle()).distinct().limit(10).toList();
    }


    public static void main(String[] args) {

    }
}

class Course {

    // online course holders
    private final String institution;
    // the unique id of each course
    private final String courseNumber;
    // the launch date of each course
    private final LocalDate launchDate;
    // the title of each course
    private final String courseTitle;
    // the instructors of each course
    private final String instructors;
    // the subject of each course
    private final String courseSubject;
    //the last time of each course
    private final int year;
    // with (1), without (0).
    private final int honorCodeCertificates;
    // the number of participants who have accessed the course
    private final int participants;
    // the number of participants who have audited more than 50% of the course
    private final int audited;
    // Total number of votes
    private final int certified;
    // the percent of the audited
    private final double auditedPercent;
    // the percent of the certified
    private final double certifiedPercent;
    // the percent of the certified with accessing the course more than 50%
    private final double certifiedOfPercent;
    // the percent of playing video
    private final double playedVideoPercent;
    // the percent of posting in forum
    private final double postedInForumPercent;
    // the percent of grade higher than zero
    private final double gradeHigherThanZeroPercent;
    // total course hours(per 1000)
    private final double totalCourseHours;
    // median hours for certification
    private final double medianHoursForCertification;
    // median age of the participants
    private final double medianAge;
    // the percent of the male
    private final double malePercent;
    // the percent of the female
    private final double femalePercent;
    // the percent of bachelor's degree of higher
    private final double bachelorDegreeOrHigher;


    // self-define field
    private final int count = 1;

    public Course(String institution, String courseNumber, LocalDate launchDate, String courseTitle,
        String instructors, String courseSubject, int year, int honorCodeCertificates,
        int participants, int audited, int certified, double auditedPercent,
        double certifiedPercent,
        double certifiedOfPercent, double playedVideoPercent, double postedInForumPercent,
        double gradeHigherThanZeroPercent,
        double totalCourseHours, double medianHoursForCertification, double medianAge,
        double malePercent, double femalePercent, double bachelorDegreeOrHigher) {
        this.institution = institution;
        this.courseNumber = courseNumber;
        this.launchDate = launchDate;
        this.courseTitle = courseTitle;
        this.instructors = instructors;
        this.courseSubject = courseSubject;
        this.year = year;
        this.honorCodeCertificates = honorCodeCertificates;
        this.participants = participants;
        this.audited = audited;
        this.certified = certified;
        this.auditedPercent = auditedPercent;
        this.certifiedPercent = certifiedPercent;
        this.certifiedOfPercent = certifiedOfPercent;
        this.playedVideoPercent = playedVideoPercent;
        this.postedInForumPercent = postedInForumPercent;
        this.gradeHigherThanZeroPercent = gradeHigherThanZeroPercent;
        this.totalCourseHours = totalCourseHours;
        this.medianHoursForCertification = medianHoursForCertification;
        this.medianAge = medianAge;
        this.malePercent = malePercent;
        this.femalePercent = femalePercent;
        this.bachelorDegreeOrHigher = bachelorDegreeOrHigher;
    }

    public String getInstitution() {
        return institution;
    }

    public String getCourseNumber() {
        return courseNumber;
    }

    public LocalDate getLaunchDate() {
        return launchDate;
    }

    public String getCourseTitle() {
        return courseTitle;
    }

    public String getInstructors() {
        return instructors;
    }

    public String getCourseSubject() {
        return courseSubject;
    }

    public int getYear() {
        return year;
    }

    public int getHonorCodeCertificates() {
        return honorCodeCertificates;
    }

    public int getParticipants() {
        return participants;
    }

    public int getAudited() {
        return audited;
    }

    public int getCertified() {
        return certified;
    }

    public double getAuditedPercent() {
        return auditedPercent;
    }

    public double getCertifiedPercent() {
        return certifiedPercent;
    }

    public double getCertifiedOfPercent() {
        return certifiedOfPercent;
    }

    public double getPlayedVideoPercent() {
        return playedVideoPercent;
    }

    public double getPostedInForumPercent() {
        return postedInForumPercent;
    }

    public double getGradeHigherThanZeroPercent() {
        return gradeHigherThanZeroPercent;
    }

    public double getTotalCourseHours() {
        return totalCourseHours;
    }

    public double getMedianHoursForCertification() {
        return medianHoursForCertification;
    }

    public double getMedianAge() {
        return medianAge;
    }

    public double getMalePercent() {
        return malePercent;
    }

    public double getFemalePercent() {
        return femalePercent;
    }

    public double getBachelorDegreeOrHigher() {
        return bachelorDegreeOrHigher;
    }

    public int getCount() {
        return count;
    }
}
