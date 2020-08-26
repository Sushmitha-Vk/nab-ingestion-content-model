package au.com.nab.eda.poc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.cmr.search.SearchParameters;
import org.alfresco.service.cmr.search.SearchService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.alfresco.consulting.accelerator.bulk.cmd.AbstractBulkUpdateCommandBase;



import org.alfresco.service.cmr.search.QueryConsistency;
import org.alfresco.service.namespace.QName;
import org.json.JSONException;



public class BulkUpdateByQueryCommand extends AbstractBulkUpdateCommandBase {
	private static final Log logger = LogFactory.getLog(BulkUpdateByQueryCommand.class);
	public static final String COMMAND_NAME="BULK_UPDATE_BY_QUERY";
	public static final String FIELD_NODE_REF="nodeRef";
	public static final String FIELD_QUERY="query";
	public static final String FIELD_LIMIT="limit";
	public static final String FIELD_SKIP="skip";
	public static final String FIELD_USE_DB="useDB";
	public static final String CTX_FIELD_COUNT="count";
	public static final String CTX_FIELD_FIXED_COUNT="fixed_count";
	public static final String FIELD_PROPS="properties";
	public static final String FIELD_ASPECTS="aspects";
	public static final String FIELD_DEL_PROPS="deleteProperties";
	public static final String FIELD_REM_ASPECTS="removeAspects";
	public static final String CONFIG="config";

	@Override
	public void workUnit(NodeRef nodeRef, JSONObject params, JSONObject ctx) {
		logger.debug("Attempting to Update: "+nodeRef.getId());
		bulkObjectMapperComponent.tryToAttachContent(nodeRef);
		int count = ctx.optInt(CTX_FIELD_COUNT);
		int fixed_count = ctx.optInt(CTX_FIELD_FIXED_COUNT);
		count++;
		boolean worked=false;
		JSONObject config = ctx.optJSONObject(CONFIG);
		JSONObject props = config.optJSONObject(FIELD_PROPS);
		JSONArray aspects = config.optJSONArray(FIELD_ASPECTS);
		JSONArray delProps = config.optJSONArray(FIELD_DEL_PROPS);
		JSONArray remAspects = config.optJSONArray(FIELD_REM_ASPECTS);
		if (props != null  && props.names() != null) {
			JSONArray propNames = props.names();
			for (int i = 0;i < propNames.length();i++) {
				worked=true;
				nodeService.setProperty(nodeRef, QName.createQName(propNames.optString(i), serviceRegistry.getNamespaceService()), props.optString(propNames.optString(i)));
			}
		}
		if (aspects != null) {
			for (int i = 0; i < aspects.length(); i++) {
				worked=true;
				nodeService.addAspect(nodeRef, QName.createQName(aspects.optString(i), serviceRegistry.getNamespaceService()), null);				
			}
		}
		if (delProps != null) {
			for (int i = 0; i < delProps.length(); i++) {
				worked=true;
				nodeService.removeProperty(nodeRef, QName.createQName(delProps.optString(i), serviceRegistry.getNamespaceService()));				
			}
		}
		if (remAspects != null) {
			for (int i = 0; i < remAspects.length(); i++) {
				worked=true;
				nodeService.removeAspect(nodeRef, QName.createQName(remAspects.optString(i), serviceRegistry.getNamespaceService()));				
			}
		}
		if (worked) {
			fixed_count++;
		}
		try {
			ctx.put(CTX_FIELD_COUNT, count);
			ctx.put(CTX_FIELD_FIXED_COUNT, fixed_count);
		} catch (JSONException e) {
			logger.error("JSON ERROR",e);
		}
	}
	
	

	@Override
	public String commandName() {
		return COMMAND_NAME;
	}

	@Override
	public Map<NodeRef, JSONObject> parseJson(JSONObject data, JSONArray list,JSONObject ctx) {
		Map<NodeRef, JSONObject> map = new HashMap<NodeRef, JSONObject>();
		if (data instanceof JSONObject) {
			boolean useDB=data.optBoolean(FIELD_USE_DB, false);
			String query=data.optString(FIELD_QUERY);
			int limit=data.optInt(FIELD_LIMIT, 100000);
			int skip=data.optInt(FIELD_SKIP, 0);
			JSONObject config = new JSONObject();
			try {
				config.put(FIELD_USE_DB, useDB);
				config.put(FIELD_QUERY, query);
				config.put(FIELD_SKIP, skip);
				config.put(FIELD_LIMIT, limit);
				config.put(FIELD_PROPS, data.optJSONObject(FIELD_PROPS));
				config.put(FIELD_ASPECTS, data.optJSONArray(FIELD_ASPECTS));
				config.put(FIELD_DEL_PROPS, data.optJSONArray(FIELD_DEL_PROPS));
				config.put(FIELD_REM_ASPECTS, data.optJSONArray(FIELD_REM_ASPECTS));
				ctx.put(CONFIG, config);
			} catch (JSONException e) {
				logger.debug("Error Updating Context", e);
			}
			if (query != null && !query.isEmpty()) {
				SearchParameters sp = new SearchParameters();
				sp.addStore(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE);
				sp.setLanguage(SearchService.LANGUAGE_FTS_ALFRESCO);
				sp.setQueryConsistency(useDB?QueryConsistency.TRANSACTIONAL:QueryConsistency.EVENTUAL);
				sp.setQuery(query);
				sp.setMaxItems(limit);
				sp.setSkipCount(skip);
				logger.info("Start Search: "+query);
				List<NodeRef> nodeRefs = searchService.query(sp).getNodeRefs();
				map = new HashMap<NodeRef, JSONObject>();
				for (NodeRef nodeRef : nodeRefs) {
					map.put(nodeRef, new JSONObject());
				}
				return map;
				
			}
		}
		logger.warn("Nothing To Update");
		return map;
	}

	@Override
	public
	void postTxn(JSONObject ctx) {
		logger.info("INTERMEDIATE COUNT: " + ctx.optInt(CTX_FIELD_COUNT));
		logger.info("INTERMEDIATE FIXED COUNT: " + ctx.optInt(CTX_FIELD_FIXED_COUNT));
	}

	@Override
	public
	void preExec(JSONObject ctx) {
		try {
			ctx.put(CTX_FIELD_COUNT,0);
			ctx.put(CTX_FIELD_FIXED_COUNT,0);
		} catch (JSONException e) {
			logger.error("JSON ERROR",e);
		}
	}
	
	@Override
	public
	void postExec(JSONObject ctx) {
		logger.info("FINAL COUNT: " + ctx.optInt(CTX_FIELD_COUNT));
		logger.info("FINAL FIXED COUNT: " + ctx.optInt(CTX_FIELD_FIXED_COUNT));
	}
	
}