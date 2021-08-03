package cityblock.member.resolution.models

/**
 * A wrapper class meant to be used for the Resolver Class.
 *
 * @param groupName: Referred to as the family to which the NodeEntity belongs to. For example, "CIN" or "EMBLEM_FACETS_MBRID"
 * @param data: This holds the nodes information and is usually used as an Identifier. For example, it could hold
 *            UUID for CityblockId or String for MBR_ID or even a User object for some defined Cityblock Member.
 * @tparam I: This is the datatype for the data you wish to link across
 */
case class NodeEntity[I](groupName: String, data: I)
