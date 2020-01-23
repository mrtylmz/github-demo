package com.nttdata.txdotconnect.repository;

import com.nttdata.txdotconnect.model.entity.Parcel;

public interface ParcelRepositoryCustom {
	
	/**
	 * Fetches all the Parcel information that needs to be included in the Parcel page
	 * 
	 * @param prclId the parcel Id
	 * @return the parcel data
	 */
	Parcel findByPrclId(String prclId);
	
}
