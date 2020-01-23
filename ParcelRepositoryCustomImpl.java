package com.nttdata.txdotconnect.repository;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.springframework.stereotype.Repository;

import com.nttdata.txdotconnect.model.entity.Parcel;
import com.nttdata.txdotconnect.model.entity.RowAgencyFundResponsibility;

/**
 * This class retrieves the Parcel with all information that needs to be included in the Parcel page.
 * 
 */
@Repository
public class ParcelRepositoryCustomImpl implements ParcelRepositoryCustom {

	private static final String QUERY_PARAM = "prclId";

	@PersistenceContext
	private EntityManager entityManager;

	@SuppressWarnings("unchecked")
	@Override
	public Parcel findByPrclId(String prclId) {
		Parcel parcel = null;

		Query query = entityManager.createQuery(
				"SELECT p, r1.associatedRowProjectId, r1.associatedRowProjectCsj, p1.projId, p1.projTypeSysid, p1.distDivSysid, p1.cntySysid, p1.hwyNm, p1.ctrlSectNbr FROM Parcel p  "
						+ "LEFT JOIN RowProject r on p.rowProjSysid = r.projSysid "
						+ "LEFT JOIN RowProjectSearch r1 on r1.associatedRowProjectSysid = r.projSysid "
						+ "LEFT JOIN Project p1 on r1.projectSysid = p1.projSysid "
						+ "WHERE p.prclId = :" + QUERY_PARAM);
		query.setParameter(QUERY_PARAM, prclId);

		Object[] returnObj = (Object[]) query.getSingleResult();
		parcel = (Parcel) returnObj[0];
		String rowProjId = (String) returnObj[1];
		String csjNbr = (String) returnObj[2];
		String projId = (String) returnObj[3];
		Long projTypeSysid = (Long) returnObj[4];
		Long distDivSysid = (Long) returnObj[5];
		Long cntySysid = (Long) returnObj[6];
		String highway = (String) returnObj[7];
		String controlSection = (String) returnObj[8];
		
		query = entityManager.createQuery(
				"SELECT a from RowAgencyFundResponsibility a where rowProject.projSysid = :" + QUERY_PARAM);
		query.setParameter(QUERY_PARAM, parcel.getRowProjSysid());
		Set<RowAgencyFundResponsibility> rowAgencyFundResponsibilities = new HashSet<>(query.getResultList());
		
		parcel.setRowProjId(rowProjId);
		parcel.setRowProjCsjNbr(csjNbr);
		parcel.setProjId(projId);
		parcel.setProjTypeSysid(projTypeSysid);
		parcel.setDistDivSysid(distDivSysid);
		parcel.setCntySysid(cntySysid);
		parcel.setHighway(highway);
		parcel.setControlSection(controlSection);
		parcel.setRowAgencyFundResponsibilities(rowAgencyFundResponsibilities);

		return parcel;
	}

}
