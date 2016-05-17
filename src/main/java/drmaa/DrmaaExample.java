package drmaa;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

import java.util.List;

/**
 * Created by kyuksel on 05/02/2016.
 * DrmaaExample was modified from: http://arc.liv.ac.uk/repos/hg/sge/source/libs/jdrmaa/src/DrmaaExample.java
 * Copyright: 2001 by Sun Microsystems, Inc.
 */
public class DrmaaExample {
    private String runSingleJob(final Session session, final String pathToShapeItScript,
                                final String pathToUgerOutput, final String jobName)
            throws DrmaaException {

        JobTemplate jt = session.createJobTemplate();
        jt.setRemoteCommand(pathToShapeItScript);
        jt.setJobName(jobName);
        jt.setOutputPath(":" + pathToUgerOutput);

        final String jobId = session.runJob(jt);
        System.out.println("Job has been submitted with id " + jobId);

        session.deleteJobTemplate(jt);

        return jobId;
    }

    private List runBulkJobs(final Session session, final String pathToShapeItScript,
                             final String pathToUgerOutput, final String jobName, int start, int end, int incr)
            throws DrmaaException {

        JobTemplate jt = session.createJobTemplate();
        jt.setNativeSpecification("-cwd -shell y -b n");
        jt.setRemoteCommand(pathToShapeItScript);
        jt.setJobName(jobName);
        jt.setOutputPath(":" + pathToUgerOutput + "." + JobTemplate.PARAMETRIC_INDEX);

        final List jobIds = session.runBulkJobs(jt, start, end, incr);
        System.out.println("Jobs have been submitted with ids " + String.join(", ", jobIds));

        session.deleteJobTemplate(jt);

        return jobIds;
    }

    public static void main(String[] args) {
        SessionFactory factory = SessionFactory.getFactory();
        Session session = factory.getSession();

        try {
            session.init("");

            final String pathToShapeItScript = args[0];
            final String pathToUgerOutput = args[1];
            final boolean isBulk = Boolean.parseBoolean(args[2]);

            DrmaaExample drm = new DrmaaExample();
            if (isBulk) {
                drm.runBulkJobs(session, pathToShapeItScript, pathToUgerOutput, "ShapeItBulkJobs", 1, 3, 1);
            } else {
                drm.runSingleJob(session, pathToShapeItScript, pathToUgerOutput, "ShapeItSingleJob");
            }


            session.exit();
        } catch (DrmaaException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
