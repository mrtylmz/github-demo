package com.nttdata.txdotconnect.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.nttdata.txdotconnect.model.dto.RowProjectParcelListDto;
import com.nttdata.txdotconnect.model.entity.Parcel;
import com.nttdata.txdotconnect.util.TxdotconnectDBConst;

@Repository
public interface ParcelRepository extends JpaRepository<Parcel, Long>, ParcelRepositoryCustom {
	
	@Query(value = "SELECT p from Parcel p"
			+ " JOIN FETCH ParcelStageLib stg ON p.prclStgLibSysid = stg.prclStgLibSysid "
			+ " where p.rowProjSysid = :rowProjSysid and stg.prclStgSeqnNbr >= com.nttdata.txdotconnect.model.entity.ParcelStageLib.CREATED_SEQ_NBR  " 
			+ " order by p.prclId") 
	List<Parcel> findFromStageCreatedByRowProj(@Param("rowProjSysid") Long rowProjSysid);

	/**
	 * Fetches just the Parcel information in the Parcel table
	 * @param prclId the parcel Id
	 * @return the Parcel data
	 */
	@Query(value = "SELECT p FROM Parcel p WHERE p.prclId = :prclId")
	Parcel findSimpleByPrclId(@Param("prclId") String prclId);

	/**
	 * Fetches all the Parcels in the Parcel table based on the Row Project Sysid
	 * @param rowProjSysid The Row Project Sysid
	 * @return List of matching Parcels
	 */
	@Query(value = "SELECT p from Parcel p where p.rowProjSysid = :rowProjSysid ORDER BY p.prclSysid ASC")
	List<Parcel> findByRowProj(@Param("rowProjSysid") Long rowProjSysid);

	
	/**
	 * Get the list of "created" (not temporary) Parcels for a given Row Project with just the information needed for presentation
	 * in the summary list of related Parcels.
	 * @param rowProjSysid  Project sysid of the Row Project
	 * @return  Matching Parcel information.
	 */
	@Query(value = 
			"SELECT new com.nttdata.txdotconnect.model.dto.RowProjectParcelListDto" +
			"(p.prclSysid, p.prclId, p.prclStgLibSysid, stg.prclStgDscr, p.fullAuthFlag, p.prclAqstnDt, p.fullAuthDt, p.createdDt) " +
			"FROM Parcel p JOIN FETCH ParcelStageLib stg ON p.prclStgLibSysid = stg.prclStgLibSysid " +
			"WHERE p.rowProjSysid = :rowProjSysid and p.prclId IS NOT NULL ORDER BY p.prclSysid ASC")
	List<RowProjectParcelListDto> findCreatedParcelsByRowProjSysid(@Param("rowProjSysid") Long rowProjSysid);
	
	/**
	 * Get the list of Temporary Parcels (where parcelId is null) for a given Row Project.
	 * Note that we use gisStgPrclId as the parcelId because the parcelId (format: P00000001) has not yet been generated.
	 * @param rowProjSysid  Project sysid of the Row Project
	 * @return  Matching Parcel information.
	 */
	@Query(value = 
			"SELECT new com.nttdata.txdotconnect.model.dto.RowProjectParcelListDto" +
			"(p.prclSysid, p.prclId, p.prclStgLibSysid, stg.prclStgDscr, p.fullAuthFlag, p.prclAqstnDt, p.fullAuthDt, p.createdDt) " +
			"FROM Parcel p JOIN FETCH ParcelStageLib stg ON p.prclStgLibSysid = stg.prclStgLibSysid " +
			"WHERE p.rowProjSysid = :rowProjSysid and p.prclId IS NULL ORDER BY p.prclSysid ASC")
	List<RowProjectParcelListDto> findTempParcelsByRowProjSysid(@Param("rowProjSysid") Long rowProjSysid);
	
	/**
	 * Get the list of Parcels for a given Row Project 
	 * @param rowProjSysid  Project sysid of the Row Project
	 * @return  Matching Parcel information.
	 */
	@Query(value = 
			"SELECT new com.nttdata.txdotconnect.model.dto.RowProjectParcelListDto" +
			"(p.prclSysid, p.prclId, p.prclStgLibSysid, stg.prclStgDscr, p.fullAuthFlag, p.prclAqstnDt, p.fullAuthDt, p.createdDt) " +
			"FROM Parcel p JOIN FETCH ParcelStageLib stg ON p.prclStgLibSysid = stg.prclStgLibSysid " +
			"WHERE p.rowProjSysid = :rowProjSysid")
	List<RowProjectParcelListDto> getParcelsList(@Param("rowProjSysid") Long rowProjSysid);
	
	
	/**
	 * UPDATE PARCEL FULL AUTHORITY
	 * This sql is used for two Parcel Update statements below. It updates 
	 * Parcel Full Authority date and flag if the following conditions are met:
	 * 1) Local Agreement Flag must be true OR the Parcel has no Local Agency Funding Responsibility records
	 * 2) Legal Description Flag must be true
	 * 3) Parcel must not be in deactivated stage
	 * 4) Federal Agreement Flag on the parent Row Project must be true OR the Row Project has no Federal
	 * 		 Agency Funding Responsibility records
	 * 5) The NEPA (Environmental Clearance) date on the Construction or Alt Delivery project must not be null
	 */
	String updateParcelFullAuthoritySql = "UPDATE Parcel p set p.fullAuthFlag = '" + TxdotconnectDBConst.FLAG_TRUE + "', p.fullAuthDt = sysdate " + 
    		"WHERE p.fullAuthFlag = '" + TxdotconnectDBConst.FLAG_FALSE + "' and p.fullAuthDt is null and " +
			// 1) Local Agreement Flag must be True OR ... 
    		"(p.lclAgrmntRcvdFlag = '" + TxdotconnectDBConst.FLAG_TRUE + "' OR " +
			// ...there are no Local Agencies attached to this parcel
    		"(select count(*) from ParcelAgencyFundingResponsibility pafr, LocalGovernmentProviderLibrary l " +
    		"where pafr.parcel.prclSysid = p.prclSysid and pafr.lclGovtProvrLibSysid = l.localGovtProvrLibSysid " + 
    		"and l.orgTypeLibSysid not in " +
    		"(com.nttdata.txdotconnect.model.entity.OrgTypeLib.FEDERAL_AGENCY, " +
    		"com.nttdata.txdotconnect.model.entity.OrgTypeLib.STATE_AGENCY)) = 0) and " +
    		// 2) Legal Description flag must be True
    		"p.leglDscrRcvdFlag = '" + TxdotconnectDBConst.FLAG_TRUE + "' and " +
    		// 3) Parcel must not be in deactivated stage
    		"p.prclStgLibSysid != com.nttdata.txdotconnect.model.entity.ParcelStageLib.DEACTIVATED_SYSID and " +
    		// 4) Federal Agreement Flag on Row Project must be True OR...
    		"((SELECT fedrlAgrmntRcvdFlag from RowProject r " +
    		"WHERE r.projSysid = p.rowProjSysid) = '" + TxdotconnectDBConst.FLAG_TRUE + "' OR " +
    		"(SELECT count(*) from RowAgencyFundResponsibility rafr, LocalGovernmentProviderLibrary l where " +
    		"rafr.rowProject.projSysid = p.rowProjSysid and rafr.lclGovtProvrLibSysid = l.localGovtProvrLibSysid " + 
    		"and l.orgTypeLibSysid = com.nttdata.txdotconnect.model.entity.OrgTypeLib.FEDERAL_AGENCY) = 0) and ";
	
    /**
     * This update is called by a Scheduled task. It sets the Full Authority Date (to sysdate) and
     * Full Authority Flag (to true) when certain conditions are met. Operates on Row Projects that are Construction Projects.
     */
    @Modifying
    @Query(updateParcelFullAuthoritySql + 
    		// 5) The NEPA (Environmental Clearance) date on the Construction project must not be null
    		"(SELECT nepaClearDt from ConstructionProject c where p.rowProjSysid = c.rowProjectTypeData.projSysid) is NOT null")
    int setFullAuthorityOnConstructionRowProjects();

    /**
     * This update is called by a Scheduled task. It sets the Full Authority Date (to sysdate) and
     * Full Authority Flag (to true) when certain conditions are met. Operates on Row Projects that are AlternativeDelivery Projects.
     */
    @Modifying
    @Query(updateParcelFullAuthoritySql +
    		// 5) The NEPA (Environmental Clearance) date on the Alt Delivery project must not be null
    		"(SELECT nepaClearDt from AlternativeDeliveryProject a where p.rowProjSysid = a.rowProjectTypeData.projSysid) is NOT null")
    int setFullAuthorityOnAlternateDeliveryProjects();
}
